package com.ewoodbury.sparklet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.core.{ExecutionService, Partition, SparkletConf}
import com.ewoodbury.sparklet.runtime.SparkletRuntime

/**
 * Pins that keyed aggregations and sortBy honor the shuffle partition count the stage graph
 * already advertises: one output partition per shuffle partition, keys co-located, empties kept.
 */
class TestWideOutputPartitioning extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val originalConf = SparkletConf.get

  override def afterEach(): Unit = {
    SparkletRuntime.get.shuffle.clear()
    SparkletConf.set(originalConf)
    ()
  }

  private def shuffleN: Int = SparkletConf.get.defaultShufflePartitions

  private def partitionsOf[A](collection: DistCollection[A]): Seq[Partition[A]] =
    ExecutionService.get.executePartitions(collection.plan)

  private def assertUniqueKeys[K, V](partitions: Seq[Partition[(K, V)]]): Unit = {
    val keys = partitions.flatMap(_.data.map(_._1))
    keys.distinct.size shouldBe keys.size
  }

  "groupByKey" should "emit one output partition per shuffle partition" in {
    val collection = DistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3, "c" -> 4, "b" -> 5), 3)
    val grouped = collection.groupByKey
    val partitions = partitionsOf(grouped)

    partitions.size shouldBe shuffleN
    assertUniqueKeys(partitions)

    val collected = grouped.collect().map { case (key, values) => key -> values.toSeq }.toMap
    collected("a") should contain theSameElementsInOrderAs Seq(1, 3)
    collected("b") should contain theSameElementsInOrderAs Seq(2, 5)
    collected("c") should contain theSameElementsAs Seq(4)
  }

  it should "preserve empty partitions for an empty source" in {
    val grouped = DistCollection(Seq.empty[(String, Int)], 2).groupByKey
    val partitions = partitionsOf(grouped)

    partitions.size shouldBe shuffleN
    partitions.foreach(_.data shouldBe empty)
    grouped.collect() shouldBe empty
  }

  it should "keep the bypass path aligned with the shuffle path" in {
    val collection = DistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3, "b" -> 4), 3)
    val bypassed = collection.partitionBy[String, Int](shuffleN).groupByKey
    val shuffled = collection.groupByKey

    partitionsOf(bypassed).size shouldBe shuffleN
    partitionsOf(shuffled).size shouldBe shuffleN
    def asMap(dc: DistCollection[(String, Iterable[Int])]): Map[String, Seq[Int]] =
      dc.collect().map { case (key, values) => key -> values.toSeq }.toMap
    asMap(bypassed) shouldBe asMap(shuffled)
  }

  it should "keep partition count through a downstream narrow op" in {
    val mapped = DistCollection(Seq("a" -> 1, "a" -> 2, "b" -> 3), 3).groupByKey
      .mapValues((values: Iterable[Int]) => values.sum)
    val partitions = partitionsOf(mapped)

    partitions.size shouldBe shuffleN
    mapped.collect().toMap shouldBe Map("a" -> 3, "b" -> 3)
  }

  it should "honor defaultShufflePartitions rather than a hardcoded width" in {
    SparkletConf.set(originalConf.copy(defaultShufflePartitions = 8))
    val partitions = partitionsOf(
      DistCollection(Seq("a" -> 1, "b" -> 2, "c" -> 3, "a" -> 4), 3).groupByKey,
    )
    partitions.size shouldBe 8
  }

  "reduceByKey" should "emit one output partition per shuffle partition" in {
    val reduced = DistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3, "c" -> 4, "b" -> 5), 3)
      .reduceByKey[String, Int](_ + _)
    val partitions = partitionsOf(reduced)

    partitions.size shouldBe shuffleN
    assertUniqueKeys(partitions)
    reduced.collect().toMap shouldBe Map("a" -> 4, "b" -> 7, "c" -> 4)
  }

  it should "preserve empty partitions for an empty source" in {
    val reduced = DistCollection(Seq.empty[(String, Int)], 2).reduceByKey[String, Int](_ + _)
    val partitions = partitionsOf(reduced)

    partitions.size shouldBe shuffleN
    partitions.foreach(_.data shouldBe empty)
    reduced.collect() shouldBe empty
  }

  it should "keep the bypass path aligned with the shuffle path" in {
    val collection = DistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3, "b" -> 4), 3)
    val bypassed = collection.partitionBy[String, Int](shuffleN).reduceByKey[String, Int](_ + _)
    val shuffled = collection.reduceByKey[String, Int](_ + _)

    partitionsOf(bypassed).size shouldBe shuffleN
    partitionsOf(shuffled).size shouldBe shuffleN
    bypassed.collect().toMap shouldBe shuffled.collect().toMap
  }

  "cogroup" should "emit one output partition per shuffle partition" in {
    val left = DistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3), 2)
    val right = DistCollection(Seq("a" -> 10, "c" -> 30), 2)
    val cogrouped = left.cogroup(right)
    val partitions = partitionsOf(cogrouped)

    partitions.size shouldBe shuffleN
    assertUniqueKeys(partitions)

    val collected = cogrouped.collect().toMap
    collected should have size 3
    collected("a")._1 should contain theSameElementsAs Seq(1, 3)
    collected("a")._2 should contain theSameElementsAs Seq(10)
    collected("b")._1 should contain theSameElementsAs Seq(2)
    collected("b")._2 shouldBe empty
    collected("c")._1 shouldBe empty
    collected("c")._2 should contain theSameElementsAs Seq(30)
  }

  it should "preserve empty partitions when both sides are empty" in {
    val left = DistCollection(Seq.empty[(String, Int)], 2)
    val right = DistCollection(Seq.empty[(String, Int)], 2)
    val cogrouped = left.cogroup(right)
    val partitions = partitionsOf(cogrouped)

    partitions.size shouldBe shuffleN
    partitions.foreach(_.data shouldBe empty)
    cogrouped.collect() shouldBe empty
  }

  "sortBy" should "keep range partitions locally sorted so collect is globally ordered" in {
    val data = Seq(5, 3, 9, 1, 7, 2, 8, 6, 4)
    val sorted = DistCollection(data, 3).sortBy(identity)
    val partitions = partitionsOf(sorted)

    partitions.size shouldBe shuffleN
    partitions.foreach { partition =>
      partition.data.toSeq shouldBe partition.data.toSeq.sorted
    }
    partitions.flatMap(_.data) should contain theSameElementsInOrderAs data.sorted
  }

  it should "preserve empty partitions for an empty source" in {
    val sorted = DistCollection(Seq.empty[Int], 2).sortBy(identity)
    val partitions = partitionsOf(sorted)

    partitions.size shouldBe shuffleN
    partitions.foreach(_.data shouldBe empty)
    sorted.collect() shouldBe empty
  }

  it should "keep global order through a downstream map" in {
    val mapped = DistCollection(Seq(5, 1, 3, 2, 4), 3).sortBy(identity).map(_ * 10)
    partitionsOf(mapped).size shouldBe shuffleN
    mapped.collect().toSeq shouldBe Seq(10, 20, 30, 40, 50)
  }

  it should "take the first n elements in global order" in {
    DistCollection(Seq(5, 1, 3, 2, 4), 3).sortBy(identity).take(3) shouldBe Seq(1, 2, 3)
  }
}
