package com.ewoodbury.sparklet.execution

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.convert.ImplicitConversions.*
import scala.collection.mutable

import com.ewoodbury.sparklet.core.{Partition, PartitionId, ShuffleId}

@SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))

/**
 * Manages shuffle data between stages in the execution graph. Handles partitioning data by key and
 * storing intermediate results.
 *
 * Storage is namespaced per-thread to avoid cross-test interference under parallel test execution.
 */
object ShuffleManager:

  /**
   * Represents shuffle data organized by partition and key.
   */
  case class ShuffleData[K, V](
      partitionedData: Map[PartitionId, Seq[(K, V)]],
  )

  // In-memory storage for shuffle data (simple local implementation), scoped by thread namespace
  // so that parallel test suites do not interfere with each other when calling clear().
  private val namespaceToStorage =
    new ConcurrentHashMap[Long, ConcurrentHashMap[ShuffleId, ShuffleData[_, _]]]()
  private val nextShuffleId = new AtomicInteger(0)
  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private def currentNamespace(): Long = Thread.currentThread().getId

  private def storageForCurrentNamespace(): ConcurrentHashMap[ShuffleId, ShuffleData[_, _]] =
    namespaceToStorage.computeIfAbsent(currentNamespace(), _ => new ConcurrentHashMap())

  /**
   * Partitions data by key across the specified number of partitions. Uses simple hash-based
   * partitioning.
   */
  def partitionByKey[K, V](
      data: Seq[Partition[(K, V)]],
      numPartitions: Int,
  ): ShuffleData[K, V] = {
    // This operation is purely local to the invocation; guard with read lock to coordinate
    // against concurrent clear() calls which acquire the write lock.
    readLock.lock()
    try
      val partitionedData = mutable.Map[PartitionId, mutable.Buffer[(K, V)]]()

      // Initialize empty partitions
      for (i <- 0 until numPartitions) {
        partitionedData(PartitionId(i)) = mutable.Buffer.empty[(K, V)]
      }

      // Distribute data across partitions using hash partitioning
      for {
        partition <- data
        (key, value) <- partition.data
      } {
        val partitionId = PartitionId(math.abs(key.hashCode) % numPartitions)
        partitionedData(partitionId) += ((key, value))
      }

      ShuffleData(partitionedData.map { case (id, buffer) => (id, buffer.toSeq) }.toMap)
    finally readLock.unlock()
  }

  /**
   * Stores shuffle data and returns a shuffle ID for later retrieval.
   */
  def writeShuffleData[K, V](shuffleData: ShuffleData[K, V]): ShuffleId = {
    readLock.lock()
    try
      val shuffleId = ShuffleId(nextShuffleId.getAndIncrement())
      val storage = storageForCurrentNamespace()
      storage(shuffleId) = shuffleData
      shuffleId
    finally readLock.unlock()
  }

  /**
   * Reads shuffle data for a specific partition from a shuffle operation.
   */
  def readShufflePartition[K, V](
      shuffleId: ShuffleId,
      partitionId: PartitionId,
  ): Partition[(K, V)] = {
    readLock.lock()
    try
      val storage = storageForCurrentNamespace()
      Option(storage.get(shuffleId)) match {
        case Some(shuffleData) =>
          val typedData = shuffleData.asInstanceOf[ShuffleData[K, V]]
          val partitionData = typedData.partitionedData.getOrElse(partitionId, Seq.empty[(K, V)])
          Partition(partitionData)
        case None =>
          throw new IllegalArgumentException(s"Shuffle ID ${shuffleId.toInt} not found")
      }
    finally readLock.unlock()
  }

  /**
   * Gets the number of partitions for a shuffle operation.
   */
  def getShufflePartitionCount(shuffleId: ShuffleId): Int = {
    readLock.lock()
    try
      val storage = storageForCurrentNamespace()
      Option(storage.get(shuffleId)) match {
        case Some(shuffleData) => shuffleData.partitionedData.size
        case None => throw new IllegalArgumentException(s"Shuffle ID ${shuffleId.toInt} not found")
      }
    finally readLock.unlock()
  }

  /**
   * Clears all shuffle data in the current thread's namespace (useful for testing).
   *
   * Note: This acquires a write lock which blocks until all ongoing read/write operations
   * complete, preventing mid-flight data loss in other threads. The shuffle ID counter is not
   * reset to avoid ID reuse across concurrently running tests.
   */
  def clear(): Unit = {
    writeLock.lock()
    try
      val ns = currentNamespace()
      Option(namespaceToStorage.get(ns)).foreach(_.clear())
      () // intentionally do NOT reset nextShuffleId to avoid ID reuse during parallel tests
    finally writeLock.unlock()
  }

  /**
   * Helper method to group data by key within a partition.
   */
  def groupByKeyInPartition[K, V](partition: Partition[(K, V)]): Partition[(K, Iterable[V])] = {
    readLock.lock()
    try
      val grouped = partition.data.groupBy(_._1).map { case (k, pairs) =>
        (k, pairs.map(_._2))
      }
      Partition(grouped.toSeq)
    finally readLock.unlock()
  }

  /**
   * Helper method to reduce data by key within a partition.
   */
  def reduceByKeyInPartition[K, V](
      partition: Partition[(K, V)],
      reduceFunc: (V, V) => V,
  ): Partition[(K, V)] = {
    readLock.lock()
    try
      val reduced = partition.data.groupBy(_._1).map { case (k, pairs) =>
        val values = pairs.map(_._2)
        val reducedValue = values
          .reduceOption(reduceFunc)
          .getOrElse(
            throw new NoSuchElementException(s"No values found for key $k"),
          )
        (k, reducedValue)
      }
      Partition(reduced.toSeq)
    finally readLock.unlock()
  }
