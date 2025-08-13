package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{Partition, PartitionId}
import com.ewoodbury.sparklet.runtime.api.SparkletRuntime

class TestShuffleService extends AnyFlatSpec with Matchers {

  "ShuffleService" should "partition data by key correctly with hash partitioning" in {
    SparkletRuntime.get.shuffle.clear() // Clean state
    val partition1 = Partition(Seq("a" -> 1, "b" -> 2))
    val partition2 = Partition(Seq("a" -> 3, "c" -> 4))
    val data = Seq(partition1, partition2)

    val shuffleData = SparkletRuntime.get.shuffle.partitionByKey(data, numPartitions = 3, SparkletRuntime.get.partitioner)

    shuffleData.partitionedData.size shouldBe 3
    val allData = shuffleData.partitionedData.values.flatten.toSeq
    allData should contain theSameElementsAs Seq("a" -> 1, "b" -> 2, "a" -> 3, "c" -> 4)
    val aPartitions = shuffleData.partitionedData.filter(_._2.exists(_._1 === "a")).keys
    aPartitions.size shouldBe 1
  }

  it should "handle empty partitions in partitioning" in {
    SparkletRuntime.get.shuffle.clear()
    val emptyPartition = Partition(Seq.empty[(String, Int)])
    val data = Seq(emptyPartition)

    val shuffleData = SparkletRuntime.get.shuffle.partitionByKey(data, numPartitions = 2, SparkletRuntime.get.partitioner)

    shuffleData.partitionedData.size shouldBe 2
    shuffleData.partitionedData.values.foreach(_.shouldBe(empty))
  }

  it should "store and retrieve shuffle data correctly" in {
    SparkletRuntime.get.shuffle.clear()
    val partition = Partition(Seq("a" -> 1, "b" -> 2))
    val shuffleData = SparkletRuntime.get.shuffle.partitionByKey(Seq(partition), numPartitions = 2, SparkletRuntime.get.partitioner)

    val shuffleId = SparkletRuntime.get.shuffle.write(shuffleData)
    shuffleId.toInt should be >= 0

    val retrievedPartition0 = SparkletRuntime.get.shuffle.readPartition[String, Int](shuffleId, PartitionId(0))
    val retrievedPartition1 = SparkletRuntime.get.shuffle.readPartition[String, Int](shuffleId, PartitionId(1))

    val combinedData = retrievedPartition0.data ++ retrievedPartition1.data
    combinedData should contain theSameElementsAs Seq("a" -> 1, "b" -> 2)
  }

  it should "handle multiple shuffle operations with unique IDs" in {
    SparkletRuntime.get.shuffle.clear()
    val data1 = Seq(Partition(Seq("a" -> 1)))
    val data2 = Seq(Partition(Seq("b" -> 2)))

    val shuffleData1 = SparkletRuntime.get.shuffle.partitionByKey(data1, numPartitions = 1, SparkletRuntime.get.partitioner)
    val shuffleData2 = SparkletRuntime.get.shuffle.partitionByKey(data2, numPartitions = 1, SparkletRuntime.get.partitioner)

    val shuffleId1 = SparkletRuntime.get.shuffle.write(shuffleData1)
    val shuffleId2 = SparkletRuntime.get.shuffle.write(shuffleData2)

    shuffleId1 should not equal shuffleId2

    val retrieved1 = SparkletRuntime.get.shuffle.readPartition[String, Int](shuffleId1, PartitionId(0))
    val retrieved2 = SparkletRuntime.get.shuffle.readPartition[String, Int](shuffleId2, PartitionId(0))

    retrieved1.data should contain theSameElementsAs Seq("a" -> 1)
    retrieved2.data should contain theSameElementsAs Seq("b" -> 2)
  }

  it should "clear all shuffle data when requested" in {
    SparkletRuntime.get.shuffle.clear()
    val partition = Partition(Seq("a" -> 1))
    val shuffleData = SparkletRuntime.get.shuffle.partitionByKey(Seq(partition), numPartitions = 1, SparkletRuntime.get.partitioner)
    val shuffleId = SparkletRuntime.get.shuffle.write(shuffleData)

    noException should be thrownBy {
      SparkletRuntime.get.shuffle.readPartition[String, Int](shuffleId, PartitionId(0))
    }

    SparkletRuntime.get.shuffle.clear()

    an[IllegalArgumentException] should be thrownBy {
      SparkletRuntime.get.shuffle.readPartition[String, Int](shuffleId, PartitionId(0))
    }
  }
}


