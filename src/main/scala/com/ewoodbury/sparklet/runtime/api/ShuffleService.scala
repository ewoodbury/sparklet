package com.ewoodbury.sparklet.runtime.api

import com.ewoodbury.sparklet.core.{Partition, PartitionId, ShuffleId}

object ShuffleService:
  /**
   * Data layout for shuffle outputs keyed by `PartitionId`.
   */
  final case class ShuffleData[K, V](partitionedData: Map[PartitionId, Seq[(K, V)]])

/**
 * Service responsible for partitioning, storing, and retrieving shuffle data.
 */
trait ShuffleService:
  import ShuffleService.ShuffleData

  /**
   * Partition key-value data across `numPartitions` using the provided partitioner.
   */
  def partitionByKey[K, V](
      data: Seq[Partition[(K, V)]],
      numPartitions: Int,
      partitioner: Partitioner,
  ): ShuffleData[K, V]

  /**
   * Persist a shuffle dataset and return its `ShuffleId`.
   */
  def write[K, V](shuffleData: ShuffleData[K, V]): ShuffleId

  /**
   * Read one partition from a stored shuffle dataset.
   */
  def readPartition[K, V](id: ShuffleId, partitionId: PartitionId): Partition[(K, V)]

  /**
   * Number of partitions for a stored shuffle dataset.
   */
  def partitionCount(id: ShuffleId): Int

  /**
   * Clear all stored shuffle state (primarily for tests).
   */
  def clear(): Unit

