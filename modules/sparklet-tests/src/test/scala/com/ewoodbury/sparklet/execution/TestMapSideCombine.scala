package com.ewoodbury.sparklet.execution

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.core.{Partition, PartitionId, ShuffleId, StageId}
import com.ewoodbury.sparklet.runtime.SparkletRuntime
import com.ewoodbury.sparklet.runtime.local.{HashPartitioner, LocalShuffleService}

/**
 * Map-side combine for reduceByKey: mapper partitions are locally reduced before the shuffle
 * write when every shuffle dependent is that reduce. Mixed diamonds write raw records.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
class TestMapSideCombine extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    SparkletRuntime.get.shuffle.clear()
    ()
  }

  private def stageInfo: StageBuilder.StageInfo =
    StageBuilder.StageInfo(
      id = StageId(0),
      stage = Stage.identity,
      inputSources = Seq.empty[StageBuilder.InputSource],
      isShuffleStage = false,
      wideOp = None,
      outputPartitioning = None,
    )

  private def recordsIn(shuffle: LocalShuffleService, shuffleId: ShuffleId, n: Int) =
    (0 until n).flatMap { partitionIdx =>
      shuffle.readPartition[Any, Any](shuffleId, PartitionId(partitionIdx)).data
    }

  "handleCombinedKeyedOutput" should "shuffle one partial per key per mapper partition" in {
    val shuffle = new LocalShuffleService
    val handler = new ShuffleHandler[IO](shuffle, new HashPartitioner)
    val mapperPartitions = Seq(
      Partition(Seq("a" -> 1, "a" -> 2, "b" -> 3)),
      Partition(Seq("a" -> 4)),
    )
    val add = (a: Any, b: Any) => a.asInstanceOf[Int] + b.asInstanceOf[Int]

    val shuffleId = handler
      .handleCombinedKeyedOutput(stageInfo, mapperPartitions, numPartitions = 4, add)
      .unsafeRunSync()

    val shuffled = recordsIn(shuffle, shuffleId, 4)
    shuffled.size shouldBe 3
    shuffled.map(_._1).toSet shouldBe Set("a", "b")

    val uncombinedId = handler
      .handleKeyedOutput(stageInfo, mapperPartitions, numPartitions = 4)
      .unsafeRunSync()
    recordsIn(shuffle, uncombinedId, 4).size shouldBe 4
  }

  it should "not mutate the mapper partitions" in {
    val shuffle = new LocalShuffleService
    val handler = new ShuffleHandler[IO](shuffle, new HashPartitioner)
    val original = Seq("a" -> 1, "a" -> 2, "a" -> 3)
    val mapperPartitions = Seq(Partition(original))
    val add = (a: Any, b: Any) => a.asInstanceOf[Int] + b.asInstanceOf[Int]

    handler
      .handleCombinedKeyedOutput(stageInfo, mapperPartitions, numPartitions = 4, add)
      .unsafeRunSync()

    mapperPartitions.flatMap(_.data).toList shouldBe original
  }

  it should "leave singleton keys and empty partitions unchanged" in {
    val shuffle = new LocalShuffleService
    val handler = new ShuffleHandler[IO](shuffle, new HashPartitioner)
    val mapperPartitions = Seq(
      Partition(Seq("solo" -> 9)),
      Partition(Seq.empty[(String, Int)]),
    )
    val add = (a: Any, b: Any) => a.asInstanceOf[Int] + b.asInstanceOf[Int]

    val shuffleId = handler
      .handleCombinedKeyedOutput(stageInfo, mapperPartitions, numPartitions = 4, add)
      .unsafeRunSync()

    recordsIn(shuffle, shuffleId, 4) should contain theSameElementsAs Seq("solo" -> 9)
  }

  "reduceByKey" should "return the same result with map-side combine" in {
    val reduced = DistCollection(
      Seq("a" -> 1, "b" -> 2, "a" -> 3, "c" -> 4, "b" -> 5, "a" -> 6),
      3,
    ).reduceByKey[String, Int](_ + _)

    reduced.collect().toMap shouldBe Map("a" -> 10, "b" -> 7, "c" -> 4)
  }

  it should "reduce an empty collection to empty" in {
    DistCollection(Seq.empty[(String, Int)], 2).reduceByKey[String, Int](_ + _).collect() shouldBe empty
  }

  it should "not combine a parent that also feeds groupByKey" in {
    val base = DistCollection(Seq("a" -> 1, "a" -> 2, "a" -> 3), 1)
    val groupedSizes = base.groupByKey.map { case (key, values) => (key, values.size) }
    val reduced = base.reduceByKey[String, Int](_ + _)
    val both = groupedSizes.union(reduced)

    both.collect() should contain theSameElementsAs Seq("a" -> 3, "a" -> 6)
    groupedSizes.collect().toMap shouldBe Map("a" -> 3)
    reduced.collect().toMap shouldBe Map("a" -> 6)
  }
}
