package com.ewoodbury.sparklet.runtime.local

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.convert.ImplicitConversions.*
import scala.collection.mutable

import com.ewoodbury.sparklet.core.{Partition, PartitionId, ShuffleId}
import com.ewoodbury.sparklet.runtime.api.{Partitioner, ShuffleService}

@SuppressWarnings(Array(
  "org.wartremover.warts.MutableDataStructures"
))
final class LocalShuffleService extends ShuffleService:
  import ShuffleService.ShuffleData

  private val namespaceToStorage =
    new ConcurrentHashMap[Long, ConcurrentHashMap[ShuffleId, ShuffleData[_, _]]]()
  private val nextShuffleId = new AtomicInteger(0)
  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private def currentNamespace(): Long = Thread.currentThread().getId

  private def storageForCurrentNamespace(): ConcurrentHashMap[ShuffleId, ShuffleData[_, _]] =
    namespaceToStorage.computeIfAbsent(currentNamespace(), _ => new ConcurrentHashMap())

  def partitionByKey[K, V](
      data: Seq[Partition[(K, V)]],
      numPartitions: Int,
      partitioner: Partitioner,
  ): ShuffleData[K, V] =
    readLock.lock()
    try
      val partitionedData = mutable.Map[PartitionId, mutable.Buffer[(K, V)]]()
      for i <- 0 until numPartitions do
        partitionedData(PartitionId(i)) = mutable.Buffer.empty[(K, V)]

      for
        partition <- data
        (key, value) <- partition.data
      do
        val pid = PartitionId(partitioner.partition(key, numPartitions))
        partitionedData(pid) += ((key, value))

      ShuffleData(partitionedData.map { case (id, buf) => (id, buf.toSeq) }.toMap)
    finally readLock.unlock()

  def write[K, V](shuffleData: ShuffleData[K, V]): ShuffleId =
    readLock.lock()
    try
      val id = ShuffleId(nextShuffleId.getAndIncrement())
      val storage = storageForCurrentNamespace()
      storage(id) = shuffleData
      id
    finally readLock.unlock()

  def readPartition[K, V](id: ShuffleId, partitionId: PartitionId): Partition[(K, V)] =
    readLock.lock()
    try
      val storage = storageForCurrentNamespace()
      Option(storage.get(id)) match
        case Some(sd) =>
          val typed = sd.asInstanceOf[ShuffleData[K, V]]
          val partData = typed.partitionedData.getOrElse(partitionId, Seq.empty[(K, V)])
          Partition(partData)
        case None => throw new IllegalArgumentException(s"Shuffle ID ${id.toInt} not found")
    finally readLock.unlock()

  def partitionCount(id: ShuffleId): Int =
    readLock.lock()
    try
      val storage = storageForCurrentNamespace()
      Option(storage.get(id)) match
        case Some(sd) => sd.partitionedData.size
        case None     => throw new IllegalArgumentException(s"Shuffle ID ${id.toInt} not found")
    finally readLock.unlock()

  def clear(): Unit =
    writeLock.lock()
    try
      val ns = currentNamespace()
      Option(namespaceToStorage.get(ns)).foreach(_.clear())
      ()
    finally writeLock.unlock()

