package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.Partition

class TestShuffleManager extends AnyFlatSpec with Matchers {

  "ShuffleManager" should "partition data by key correctly with hash partitioning" in {
    ShuffleManager.clear() // Clean state
    val partition1 = Partition(Seq("a" -> 1, "b" -> 2))
    val partition2 = Partition(Seq("a" -> 3, "c" -> 4))
    val data = Seq(partition1, partition2)
    
    val shuffleData = ShuffleManager.partitionByKey(data, numPartitions = 3)
    
    // Should have the correct number of partitions
    shuffleData.partitionedData.size shouldBe 3
    
    // All original data should be preserved (just redistributed)
    val allData = shuffleData.partitionedData.values.flatten.toSeq
    allData should contain theSameElementsAs Seq("a" -> 1, "b" -> 2, "a" -> 3, "c" -> 4)
    
    // Keys with same hash should go to same partition
    val aPartitions = shuffleData.partitionedData.filter(_._2.exists(_._1 === "a")).keys
    aPartitions.size shouldBe 1 // All "a" keys should be in same partition
  }

  it should "handle empty partitions in partitioning" in {
    ShuffleManager.clear()
    val emptyPartition = Partition(Seq.empty[(String, Int)])
    val data = Seq(emptyPartition)
    
    val shuffleData = ShuffleManager.partitionByKey(data, numPartitions = 2)
    
    shuffleData.partitionedData.size shouldBe 2
    shuffleData.partitionedData.values.foreach(_.shouldBe(empty))
  }

  it should "store and retrieve shuffle data correctly" in {
    ShuffleManager.clear()
    val partition = Partition(Seq("a" -> 1, "b" -> 2))
    val shuffleData = ShuffleManager.partitionByKey(Seq(partition), numPartitions = 2)
    
    val shuffleId = ShuffleManager.writeShuffleData(shuffleData)
    
    // Should return a valid shuffle ID
    shuffleId should be >= 0
    
    // Should be able to retrieve the data
    val retrievedPartition0 = ShuffleManager.readShufflePartition[String, Int](shuffleId, 0)
    val retrievedPartition1 = ShuffleManager.readShufflePartition[String, Int](shuffleId, 1)
    
    // Combined data should match original
    val combinedData = retrievedPartition0.data ++ retrievedPartition1.data
    combinedData should contain theSameElementsAs Seq("a" -> 1, "b" -> 2)
  }

  it should "handle multiple shuffle operations with unique IDs" in {
    ShuffleManager.clear()
    val data1 = Seq(Partition(Seq("a" -> 1)))
    val data2 = Seq(Partition(Seq("b" -> 2)))
    
    val shuffleData1 = ShuffleManager.partitionByKey(data1, numPartitions = 1)
    val shuffleData2 = ShuffleManager.partitionByKey(data2, numPartitions = 1)
    
    val shuffleId1 = ShuffleManager.writeShuffleData(shuffleData1)
    val shuffleId2 = ShuffleManager.writeShuffleData(shuffleData2)
    
    // Should get different shuffle IDs
    shuffleId1 should not equal shuffleId2
    
    // Should be able to retrieve both datasets independently
    val retrieved1 = ShuffleManager.readShufflePartition[String, Int](shuffleId1, 0)
    val retrieved2 = ShuffleManager.readShufflePartition[String, Int](shuffleId2, 0)
    
    retrieved1.data should contain theSameElementsAs Seq("a" -> 1)
    retrieved2.data should contain theSameElementsAs Seq("b" -> 2)
  }

  it should "group data by key within a partition correctly" in {
    ShuffleManager.clear()
    val partition = Partition(Seq("a" -> 1, "b" -> 2, "a" -> 3, "b" -> 4))
    
    val grouped = ShuffleManager.groupByKeyInPartition(partition)
    
    grouped.data should have size 2
    val dataMap = grouped.data.toMap
    dataMap("a") should contain theSameElementsAs Seq(1, 3)
    dataMap("b") should contain theSameElementsAs Seq(2, 4)
  }

  it should "reduce data by key within a partition correctly" in {
    ShuffleManager.clear()
    val partition = Partition(Seq("a" -> 1, "b" -> 2, "a" -> 3, "b" -> 4))
    
    val reduced = ShuffleManager.reduceByKeyInPartition(partition, (a: Int, b: Int) => a + b)
    
    reduced.data should have size 2
    val dataMap = reduced.data.toMap
    dataMap("a") shouldBe 4  // 1 + 3
    dataMap("b") shouldBe 6  // 2 + 4
  }

  it should "handle empty partitions in grouping operations" in {
    ShuffleManager.clear()
    val emptyPartition = Partition(Seq.empty[(String, Int)])
    
    val grouped = ShuffleManager.groupByKeyInPartition(emptyPartition)
    val reduced = ShuffleManager.reduceByKeyInPartition(emptyPartition, (a: Int, b: Int) => a + b)
    
    grouped.data shouldBe empty
    reduced.data shouldBe empty
  }

  it should "clear all shuffle data when requested" in {
    ShuffleManager.clear()
    val partition = Partition(Seq("a" -> 1))
    val shuffleData = ShuffleManager.partitionByKey(Seq(partition), numPartitions = 1)
    val shuffleId = ShuffleManager.writeShuffleData(shuffleData)
    
    // Verify data exists
    noException should be thrownBy {
      ShuffleManager.readShufflePartition[String, Int](shuffleId, 0)
    }
    
    // Clear and verify data is gone
    ShuffleManager.clear()
    
    an[IllegalArgumentException] should be thrownBy {
      ShuffleManager.readShufflePartition[String, Int](shuffleId, 0)
    }
  }
}