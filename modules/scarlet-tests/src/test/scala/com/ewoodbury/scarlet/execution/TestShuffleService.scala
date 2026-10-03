package com.ewoodbury.scarlet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.{Partition, PartitionId}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

class TestShuffleService extends AnyFlatSpec with Matchers {

  "ShuffleService" should "partition data by key correctly with hash partitioning" in {
    ScarletRuntime.get.shuffle.clear() // Clean state
    val partition1 = Partition(Seq("a" -> 1, "b" -> 2))
    val partition2 = Partition(Seq("a" -> 3, "c" -> 4))
    val data = Seq(partition1, partition2)

    val shuffleData = ScarletRuntime.get.shuffle.partitionByKey(data, numPartitions = 3, ScarletRuntime.get.partitioner)

    shuffleData.partitionedData.size shouldBe 3
    val allData = shuffleData.partitionedData.values.flatten.toSeq
    allData should contain theSameElementsAs Seq("a" -> 1, "b" -> 2, "a" -> 3, "c" -> 4)
    val aPartitions = shuffleData.partitionedData.filter(_._2.exists(_._1 === "a")).keys
    aPartitions.size shouldBe 1
  }

  it should "handle empty partitions in partitioning" in {
    ScarletRuntime.get.shuffle.clear()
    val emptyPartition = Partition(Seq.empty[(String, Int)])
    val data = Seq(emptyPartition)

    val shuffleData = ScarletRuntime.get.shuffle.partitionByKey(data, numPartitions = 2, ScarletRuntime.get.partitioner)

    shuffleData.partitionedData.size shouldBe 2
    shuffleData.partitionedData.values.foreach(_.shouldBe(empty))
  }

  it should "store and retrieve shuffle data correctly" in {
    ScarletRuntime.get.shuffle.clear()
    val partition = Partition(Seq("a" -> 1, "b" -> 2))
    val shuffleData = ScarletRuntime.get.shuffle.partitionByKey(Seq(partition), numPartitions = 2, ScarletRuntime.get.partitioner)

    val shuffleId = ScarletRuntime.get.shuffle.write(shuffleData)
    shuffleId.toInt should be >= 0

    val retrievedPartition0 = ScarletRuntime.get.shuffle.readPartition[String, Int](shuffleId, PartitionId(0))
    val retrievedPartition1 = ScarletRuntime.get.shuffle.readPartition[String, Int](shuffleId, PartitionId(1))

    val combinedData = retrievedPartition0.data ++ retrievedPartition1.data
    combinedData should contain theSameElementsAs Seq("a" -> 1, "b" -> 2)
  }

  it should "handle multiple shuffle operations with unique IDs" in {
    ScarletRuntime.get.shuffle.clear()
    val data1 = Seq(Partition(Seq("a" -> 1)))
    val data2 = Seq(Partition(Seq("b" -> 2)))

    val shuffleData1 = ScarletRuntime.get.shuffle.partitionByKey(data1, numPartitions = 1, ScarletRuntime.get.partitioner)
    val shuffleData2 = ScarletRuntime.get.shuffle.partitionByKey(data2, numPartitions = 1, ScarletRuntime.get.partitioner)

    val shuffleId1 = ScarletRuntime.get.shuffle.write(shuffleData1)
    val shuffleId2 = ScarletRuntime.get.shuffle.write(shuffleData2)

    shuffleId1 should not equal shuffleId2

    val retrieved1 = ScarletRuntime.get.shuffle.readPartition[String, Int](shuffleId1, PartitionId(0))
    val retrieved2 = ScarletRuntime.get.shuffle.readPartition[String, Int](shuffleId2, PartitionId(0))

    retrieved1.data should contain theSameElementsAs Seq("a" -> 1)
    retrieved2.data should contain theSameElementsAs Seq("b" -> 2)
  }

  it should "clear all shuffle data when requested" in {
    ScarletRuntime.get.shuffle.clear()
    val partition = Partition(Seq("a" -> 1))
    val shuffleData = ScarletRuntime.get.shuffle.partitionByKey(Seq(partition), numPartitions = 1, ScarletRuntime.get.partitioner)
    val shuffleId = ScarletRuntime.get.shuffle.write(shuffleData)

    noException should be thrownBy {
      ScarletRuntime.get.shuffle.readPartition[String, Int](shuffleId, PartitionId(0))
    }

    ScarletRuntime.get.shuffle.clear()

    an[IllegalArgumentException] should be thrownBy {
      ScarletRuntime.get.shuffle.readPartition[String, Int](shuffleId, PartitionId(0))
    }
  }
}


