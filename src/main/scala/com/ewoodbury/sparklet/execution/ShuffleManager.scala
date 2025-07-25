package com.ewoodbury.sparklet.execution

import scala.collection.mutable

import com.ewoodbury.sparklet.core.Partition


/**
 * Manages shuffle data between stages in the execution graph.
 * Handles partitioning data by key and storing intermediate results.
 */
object ShuffleManager:
  
  /**
   * Represents shuffle data organized by partition and key.
   */
  case class ShuffleData[K, V](
    partitionedData: Map[Int, Seq[(K, V)]]
  )
  
  // In-memory storage for shuffle data (simple local implementation)
  private val shuffleStorage = mutable.Map[Int, ShuffleData[_, _]]()
  private var nextShuffleId = 0
  
  /**
   * Partitions data by key across the specified number of partitions.
   * Uses simple hash-based partitioning.
   */
  def partitionByKey[K, V](
    data: Seq[Partition[(K, V)]], 
    numPartitions: Int
  ): ShuffleData[K, V] = {
    val partitionedData = mutable.Map[Int, mutable.Buffer[(K, V)]]()
    
    // Initialize empty partitions
    for (i <- 0 until numPartitions) {
      partitionedData(i) = mutable.Buffer.empty[(K, V)]
    }
    
    // Distribute data across partitions using hash partitioning
    for {
      partition <- data
      (key, value) <- partition.data
    } {
      val partitionId = math.abs(key.hashCode) % numPartitions
      partitionedData(partitionId) += ((key, value))
    }
    
    ShuffleData(partitionedData.map { case (id, buffer) => (id, buffer.toSeq) }.toMap)
  }
  
  /**
   * Stores shuffle data and returns a shuffle ID for later retrieval.
   */
  def writeShuffleData[K, V](shuffleData: ShuffleData[K, V]): Int = {
    val shuffleId = nextShuffleId
    nextShuffleId += 1
    shuffleStorage(shuffleId) = shuffleData
    shuffleId
  }
  
  /**
   * Reads shuffle data for a specific partition from a shuffle operation.
   */
  def readShufflePartition[K, V](shuffleId: Int, partitionId: Int): Partition[(K, V)] = {
    shuffleStorage.get(shuffleId) match {
      case Some(shuffleData) =>
        val typedData = shuffleData.asInstanceOf[ShuffleData[K, V]]
        val partitionData = typedData.partitionedData.getOrElse(partitionId, Seq.empty)
        Partition(partitionData)
      case None =>
        throw new IllegalArgumentException(s"Shuffle ID $shuffleId not found")
    }
  }
  
  /**
   * Gets the number of partitions for a shuffle operation.
   */
  def getShufflePartitionCount(shuffleId: Int): Int = {
    shuffleStorage.get(shuffleId) match {
      case Some(shuffleData) => shuffleData.partitionedData.size
      case None => throw new IllegalArgumentException(s"Shuffle ID $shuffleId not found")
    }
  }
  
  /**
   * Clears all shuffle data (useful for testing).
   */
  def clear(): Unit = {
    shuffleStorage.clear()
    nextShuffleId = 0
  }
  
  /**
   * Helper method to group data by key within a partition.
   */
  def groupByKeyInPartition[K, V](partition: Partition[(K, V)]): Partition[(K, Iterable[V])] = {
    val grouped = partition.data.groupBy(_._1).map { case (k, pairs) => 
      (k, pairs.map(_._2)) 
    }
    Partition(grouped.toSeq)
  }
  
  /**
   * Helper method to reduce data by key within a partition.
   */
  def reduceByKeyInPartition[K, V](
    partition: Partition[(K, V)], 
    reduceFunc: (V, V) => V
  ): Partition[(K, V)] = {
    val reduced = partition.data.groupBy(_._1).map { case (k, pairs) =>
      val values = pairs.map(_._2)
      val reducedValue = values.reduceOption(reduceFunc).getOrElse(
        throw new NoSuchElementException(s"No values found for key $k")
      )
      (k, reducedValue)
    }
    Partition(reduced.toSeq)
  } 