package com.ewoodbury.sparklet.runtime.local

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import com.ewoodbury.sparklet.core.{Partition, PartitionId, ShuffleId}
import com.ewoodbury.sparklet.runtime.api.{Partitioner, ShuffleService}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.MutableDataStructures",
  ),
)
final class LocalShuffleService extends ShuffleService:
  import ShuffleService.ShuffleData

  // Global storage shared across all threads to ensure writers/readers see the same data
  private val storage = new ConcurrentHashMap[ShuffleId, ShuffleData[_, _]]()
  private val nextShuffleId = new AtomicInteger(0)

  def partitionByKey[K, V](
      data: Seq[Partition[(K, V)]],
      numPartitions: Int,
      partitioner: Partitioner,
  ): ShuffleData[K, V] =
    val partitionedData = mutable.Map[PartitionId, mutable.Buffer[(K, V)]]()
    for i <- 0 until math.max(numPartitions, 0) do
      partitionedData(PartitionId(i)) = mutable.Buffer.empty[(K, V)]

    for
      partition <- data
      (key, value) <- partition.data
    do
      val pid = PartitionId(partitioner.partition(key, numPartitions))
      partitionedData(pid) += ((key, value))

    ShuffleData(partitionedData.map { case (id, buf) => (id, buf.toSeq) }.toMap)

  def write[K, V](shuffleData: ShuffleData[K, V]): ShuffleId =
    val id = ShuffleId(nextShuffleId.getAndIncrement())
    storage.put(id, shuffleData)
    id

  def readPartition[K, V](id: ShuffleId, partitionId: PartitionId): Partition[(K, V)] =
    Option(storage.get(id)) match
      case Some(sd) =>
        val typed = sd.asInstanceOf[ShuffleData[K, V]]
        val partData = typed.partitionedData.getOrElse(partitionId, Seq.empty[(K, V)])
        Partition(partData)
      case None => throw new IllegalArgumentException(s"Shuffle ID ${id.toInt} not found")

  def partitionCount(id: ShuffleId): Int =
    Option(storage.get(id)) match
      case Some(sd) => sd.partitionedData.size
      case None => throw new IllegalArgumentException(s"Shuffle ID ${id.toInt} not found")

  def clear(): Unit = storage.clear()
