package com.ewoodbury.sparklet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.columnar.BatchCodec
import com.ewoodbury.sparklet.core.{ExecutionService, Partition, Plan, SparkletConf}
import com.ewoodbury.sparklet.runtime.SparkletRuntime

/**
 * Pins which plans select the columnar kernels, and that the row path still owns everything else.
 */
class TestColumnarSelection extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = SparkletConf.get

  override def afterEach(): Unit =
    SparkletRuntime.get.shuffle.clear()
    SparkletConf.set(originalConf)
    ()

  "int filter" should "keep input order, partition count, and a columnar layout" in {
    val filtered = DistCollection(Seq(1, 2, 3, 4, 5), 2).filter(_ % 2 == 0)
    filtered.collect() shouldBe Seq(2, 4)
    partitionsOf(filtered).size shouldBe 2
    assertNarrow(filtered, 2)
  }

  it should "keep order when map and filter run as morsels" in {
    SparkletConf.set(SparkletConf.get.copy(columnarBatchSize = 2, threadPoolSize = 4))
    val mapped = DistCollection(Seq(1, 2, 3, 4, 5), 1).map(_ * 2).filter(_ > 5)
    mapped.collect() shouldBe Seq(6, 8, 10)
    partitionsOf(mapped).size shouldBe 1
    assertNarrow(mapped, 1)
  }

  it should "stay columnar and empty when every row is dropped" in {
    val dropped = DistCollection(Seq(1, 2, 3), 3).filter(_ > 10)
    dropped.collect() shouldBe empty
    val parts = partitionsOf(dropped)
    parts.size shouldBe 3
    parts.forall(_.data.isEmpty) shouldBe true
    assertNarrow(dropped, 3)
  }

  it should "classify ints when some source partitions are empty" in {
    val source = Plan.Source(
      Seq(
        Partition(Seq.empty[Int]),
        Partition(Seq(4, 5)),
        Partition(Seq.empty[Int]),
      ),
    )
    val filtered = Plan.FilterOp(source, (value: Int) => value > 0)
    val info = ColumnarPlanner.partitioning(filtered).getOrElse(fail("empty partitions hid the ints"))
    info.distribution shouldBe Distribution.Unknown(3)
    info.layout shouldBe Layout.Columnar(SparkletConf.get.columnarBatchSize)
    DistCollection(filtered).collect() shouldBe Seq(4, 5)
    partitionsOf(DistCollection(filtered)).map(_.data.toSeq) shouldBe Seq(
      Seq.empty[Int],
      Seq(4, 5),
      Seq.empty[Int],
    )
  }

  "int map to string" should "return strings on the row path" in {
    DistCollection(Seq(1, 2, 3), 2).map(_.toString).collect() shouldBe Seq("1", "2", "3")
  }

  "string keys" should "keep reduceByKey and join on the row path" in {
    val reduced = DistCollection(Seq("a" -> 1, "a" -> 2, "b" -> 3), 2).reduceByKey[String, Int](_ + _)
    reduced.collect().toMap shouldBe Map("a" -> 3, "b" -> 3)
    infoOf(reduced) shouldBe Option.empty[PartitioningInfo]

    val joined = DistCollection(Seq("a" -> 1), 1).join(DistCollection(Seq("a" -> 10), 1))
    joined.collect().toSeq should contain theSameElementsAs Seq("a" -> ((1, 10)))
    infoOf(joined) shouldBe Option.empty[PartitioningInfo]
  }

  "union of int maps" should "stay on the row path" in {
    val left = DistCollection(Seq(1, 2, 3), 2).map(_ * 2)
    val right = DistCollection(Seq(4, 5), 1).filter(_ % 2 == 1)
    val united = left.union(right)
    united.collect() shouldBe Seq(2, 4, 6, 5)
    infoOf(united) shouldBe Option.empty[PartitioningInfo]
    assertNarrow(left, 2)
  }

  "int reduceByKey" should "hash into the shuffle width and apply the user operator" in {
    SparkletConf.set(SparkletConf.get.copy(columnarBatchSize = 2, threadPoolSize = 4))
    val rows = Seq(0 -> 1, 0 -> 2, 4 -> 3, 1 -> Int.MaxValue, 1 -> 10, Int.MinValue -> 8, Int.MinValue -> -3)
    val reduced = DistCollection(rows, 3).reduceByKey[Int, Int](_ + _)
    val width = SparkletConf.get.defaultShufflePartitions

    reduced.collect().toMap shouldBe Map(
      0 -> 3,
      4 -> 3,
      1 -> (Int.MaxValue + 10),
      Int.MinValue -> 5,
    )
    val parts = partitionsOf(reduced)
    parts.size shouldBe width
    parts.exists(_.data.isEmpty) shouldBe true
    assertBuckets(parts)
    assertUniqueKeys(parts)
    assertHashed(reduced)
  }

  it should "apply max rather than a sum" in {
    val rows = Seq(1 -> 3, 1 -> 9, 1 -> 4, 2 -> 1, 2 -> 8)
    val reduced = DistCollection(rows, 2).reduceByKey[Int, Int](math.max)
    reduced.collect().toMap shouldBe Map(1 -> 9, 2 -> 8)
    assertHashed(reduced)
  }

  it should "keep a hash layout through a later filter" in {
    val filtered = DistCollection(Seq(1 -> 1, 2 -> 2, 1 -> 3, 5 -> 1), 2)
      .reduceByKey[Int, Int](_ + _)
      .filter(_._2 > 1)
    filtered.collect().toMap shouldBe Map(1 -> 4, 2 -> 2)
    assertHashed(filtered)
    val parts = partitionsOf(filtered)
    assertBuckets(parts)
    assertUniqueKeys(parts)
  }

  "int pairs" should "filter keys and values in input order" in {
    val pairs = DistCollection(Seq(1 -> 10, 2 -> 20, 3 -> 5, 4 -> 1), 2)
    pairs.filterKeys[Int, Int](_ % 2 == 0).collect() shouldBe Seq(2 -> 20, 4 -> 1)
    pairs.filterValues[Int, Int](_ > 5).collect() shouldBe Seq(1 -> 10, 2 -> 20)
    pairs.filter { case (_, value) => value > 5 }.collect() shouldBe Seq(1 -> 10, 2 -> 20)
    assertNarrow(pairs.filterKeys[Int, Int](_ > 0), 2)
  }

  "int join" should "match the pair multiset and the shuffle width" in {
    SparkletConf.set(SparkletConf.get.copy(columnarBatchSize = 2, threadPoolSize = 4))
    val build = DistCollection(Seq(1 -> 10, 1 -> 11, 2 -> 20, 4 -> 40), 2)
    val probe = DistCollection(Seq(1 -> 100, 2 -> 200, 1 -> 101, 3 -> 30), 2)
    val joined = build.join(probe)
    val triples = joined.collect().map { case (key, (left, right)) => (key, left, right) }.toSeq

    triples should contain theSameElementsAs Seq(
      (1, 10, 100),
      (1, 11, 100),
      (1, 10, 101),
      (1, 11, 101),
      (2, 20, 200),
    )
    val parts = partitionsOf(joined)
    parts.size shouldBe SparkletConf.get.defaultShufflePartitions
    assertBuckets(parts)
    assertHashed(joined)
  }

  it should "leave a later filter on the row path" in {
    val build = DistCollection(Seq(1 -> 10, 2 -> 20), 1)
    val probe = DistCollection(Seq(1 -> 7, 3 -> 8), 1)
    val filtered = build.join(probe).filter { case (_, (left, _)) => left > 0 }
    filtered.collect().toSeq should contain theSameElementsAs Seq(1 -> ((10, 7)))
    infoOf(filtered) shouldBe Option.empty[PartitioningInfo]
    assertHashed(build.join(probe))
  }

  "string filter" should "keep input order, nulls, and a columnar layout" in {
    val filtered = DistCollection(texts(Some("a"), None, Some("bb"), Some("a"), Some("")), 2).filter {
      value =>
        Option(value) match
          case Some("bb") => false
          case _ => true
    }

    filtered.collect() shouldBe texts(Some("a"), None, Some("a"), Some(""))
    assertNarrow(filtered, 2)
  }

  it should "keep order when map and filter run as morsels" in {
    SparkletConf.set(SparkletConf.get.copy(columnarBatchSize = 2, threadPoolSize = 4))
    val mapped = DistCollection(Seq("a", "bb", "ccc", "d", "ee"), 1)
      .map(_.toUpperCase(java.util.Locale.ENGLISH))
      .filter(_.length > 1)

    mapped.collect() shouldBe Seq("BB", "CCC", "EE")
    assertNarrow(mapped, 1)
  }

  it should "select strings that start with null, and leave an all-null source on the row path" in {
    val witnessed =
      DistCollection(texts(None, Some("a"), Some("b")), 1).filter(value => Option(value).isDefined)
    witnessed.collect() shouldBe Seq("a", "b")
    assertNarrow(witnessed, 1)

    val unseen = DistCollection(texts(None, None), 2).filter(_ => true)
    unseen.collect() shouldBe texts(None, None)
    infoOf(unseen) shouldBe Option.empty[PartitioningInfo]
  }

  it should "match the row path when a map introduces and removes nulls" in {
    val plan = DistCollection(texts(Some("a"), None, Some("bb"), Some("a")), 2).map { value =>
      Option(value) match
        case None => "n"
        case Some("bb") => text(None)
        case Some(present) => present.toUpperCase(java.util.Locale.ENGLISH)
    }

    assertNarrow(plan, 2)
    columnar(plan.collect()) shouldBe row(plan.collect())
  }

  it should "fall back when a string map does not return a string" in {
    val mapped = DistCollection(Seq("a", "bb"), 2).map(_.length)

    mapped.collect() shouldBe Seq(1, 2)
  }

  "columnar path" should "match the row path on filter, map, reduceByKey, join, and filterKeys" in {
    val ints = Seq(1, 2, 3, 4, 5, 6)
    val pairs = Seq(1 -> 10, 2 -> 20, 1 -> 3, 4 -> 1, 2 -> 5)
    val build = Seq(1 -> 10, 1 -> 11, 2 -> 20, 4 -> 1)
    val probe = Seq(1 -> 100, 2 -> 200, 3 -> 30, 1 -> 101)

    def run[A](flag: Boolean)(body: => A): A =
      SparkletRuntime.get.shuffle.clear()
      SparkletConf.set(originalConf.copy(columnarExecution = flag))
      body

    run(true)(DistCollection(ints, 2).filter(_ % 2 == 0).collect()) shouldBe
      run(false)(DistCollection(ints, 2).filter(_ % 2 == 0).collect())
    run(true)(DistCollection(ints, 2).map(_ * 2).filter(_ > 5).collect()) shouldBe
      run(false)(DistCollection(ints, 2).map(_ * 2).filter(_ > 5).collect())
    run(true)(DistCollection(pairs, 3).reduceByKey[Int, Int](_ + _).collect().toMap) shouldBe
      run(false)(DistCollection(pairs, 3).reduceByKey[Int, Int](_ + _).collect().toMap)
    run(true)(DistCollection(pairs, 2).filterKeys[Int, Int](_ % 2 == 0).collect()) shouldBe
      run(false)(DistCollection(pairs, 2).filterKeys[Int, Int](_ % 2 == 0).collect())

    val columnarJoin = run(true) {
      DistCollection(build, 2).join(DistCollection(probe, 2)).collect().toSeq.sorted
    }
    val rowJoin = run(false) {
      DistCollection(build, 2).join(DistCollection(probe, 2)).collect().toSeq.sorted
    }
    columnarJoin shouldBe rowJoin
  }

  "columnarExecution" should "force the row path when it is false" in {
    SparkletConf.set(SparkletConf.get.copy(columnarExecution = false))
    val filtered = DistCollection(Seq(1, 2, 3, 4), 2).filter(_ % 2 == 0)
    filtered.collect() shouldBe Seq(2, 4)
    infoOf(filtered) shouldBe Option.empty[PartitioningInfo]
  }

  private def columnar[A](body: => A): A =
    SparkletRuntime.get.shuffle.clear()
    SparkletConf.set(originalConf.copy(columnarExecution = true))
    body

  private def row[A](body: => A): A =
    SparkletRuntime.get.shuffle.clear()
    SparkletConf.set(originalConf.copy(columnarExecution = false))
    body

  private def partitionsOf[A](collection: DistCollection[A]): Seq[Partition[A]] =
    ExecutionService.get.executePartitions(collection.plan)

  private def infoOf[A](collection: DistCollection[A]): Option[PartitioningInfo] =
    ColumnarPlanner.partitioning(collection.plan)

  private def assertNarrow[A](collection: DistCollection[A], width: Int): PartitioningInfo =
    val info = infoOf(collection).getOrElse(fail("expected a columnar plan"))
    info.distribution shouldBe Distribution.Unknown(width)
    info.ordering shouldBe OrderingTag.Unsorted
    info.layout shouldBe Layout.Columnar(SparkletConf.get.columnarBatchSize)
    info

  private def assertHashed[A](collection: DistCollection[A]): PartitioningInfo =
    val info = infoOf(collection).getOrElse(fail("expected a columnar plan"))
    info.distribution shouldBe Distribution.Hash(
      SparkletConf.get.defaultShufflePartitions,
      PartitioningInfo.RowKeyOrdinals,
    )
    info.ordering shouldBe OrderingTag.Unsorted
    info.layout shouldBe Layout.Columnar(SparkletConf.get.columnarBatchSize)
    info

  private def assertBuckets[V](parts: Seq[Partition[(Int, V)]]): Unit =
    val width = parts.length
    parts.zipWithIndex.foreach { (partition, index) =>
      partition.data.foreach { (key, _) =>
        Math.floorMod(key, width) shouldBe index
      }
    }

  private def text(cell: Option[String]): String = BatchCodec.stringElement(cell)

  private def texts(cells: Option[String]*): Seq[String] = cells.map(text)

  private def assertUniqueKeys[V](parts: Seq[Partition[(Int, V)]]): Unit =
    val keys = parts.flatMap(_.data.map(_._1))
    keys.distinct.size shouldBe keys.size
    ()
