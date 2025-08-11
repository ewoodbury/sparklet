package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.{Partition, PartitionId, ShuffleId}
import com.ewoodbury.sparklet.runtime.api.{ShuffleService as SPIShuffleService, SparkletRuntime}

/**
 * Compatibility shim that forwards shuffle operations to the active runtime's ShuffleService.
 *
 * This preserves the existing API surface while enabling pluggable shuffle backends.
 */
object ShuffleManager:

  /**
   * Represents shuffle data organized by partition and key.
   * Kept for backward compatibility; converted to the SPI type internally.
   */
  case class ShuffleData[K, V](partitionedData: Map[PartitionId, Seq[(K, V)]])

  def partitionByKey[K, V](
      data: Seq[Partition[(K, V)]],
      numPartitions: Int,
  ): ShuffleData[K, V] =
    val sd = SparkletRuntime
      .get
      .shuffle
      .partitionByKey(data, numPartitions, SparkletRuntime.get.partitioner)
    ShuffleData(sd.partitionedData)

  def writeShuffleData[K, V](shuffleData: ShuffleData[K, V]): ShuffleId =
    val sd = SPIShuffleService.ShuffleData(shuffleData.partitionedData)
    SparkletRuntime.get.shuffle.write(sd)

  def readShufflePartition[K, V](
      shuffleId: ShuffleId,
      partitionId: PartitionId,
  ): Partition[(K, V)] =
    SparkletRuntime.get.shuffle.readPartition(shuffleId, partitionId)

  def getShufflePartitionCount(shuffleId: ShuffleId): Int =
    SparkletRuntime.get.shuffle.partitionCount(shuffleId)

  def clear(): Unit =
    SparkletRuntime.get.shuffle.clear()

  /**
   * Helper method to group data by key within a partition.
   */
  def groupByKeyInPartition[K, V](partition: Partition[(K, V)]): Partition[(K, Iterable[V])] =
    val grouped = partition.data.groupBy(_._1).map { case (k, pairs) =>
      (k, pairs.map(_._2))
    }
    Partition(grouped.toSeq)

  /**
   * Helper method to reduce data by key within a partition.
   */
  def reduceByKeyInPartition[K, V](
      partition: Partition[(K, V)],
      reduceFunc: (V, V) => V,
  ): Partition[(K, V)] =
    val reduced = partition.data.groupBy(_._1).map { case (k, pairs) =>
      val values = pairs.map(_._2)
      val reducedValue = values
        .reduceOption(reduceFunc)
        .getOrElse(throw new NoSuchElementException(s"No values found for key $k"))
      (k, reducedValue)
    }
    Partition(reduced.toSeq)
