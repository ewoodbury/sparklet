package com.ewoodbury.scarlet.execution

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.scarlet.core.{Partition, PartitionId, Plan, ScarletConf, ShuffleId}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.api.{ShuffleService, TaskScheduler}

/**
 * Executor for handling different join strategies.
 *
 * Named erasure boundary: join tasks operate on co-partitioned shuffle reads whose record types
 * are erased in stage transport; task results are cast back to the partition sequence the stage
 * graph expects. Safe because every task in a submission produces the same record type as the
 * stage it belongs to.
 */
final class JoinExecutor[F[_]: Sync](
    shuffle: ShuffleService,
    scheduler: TaskScheduler[F],
) extends StrictLogging:

  /**
   * Select the best join strategy based on data size heuristics.
   */
  def selectJoinStrategy(
      leftShuffleId: ShuffleId,
      rightShuffleId: ShuffleId,
  ): Plan.JoinStrategy = {
    val leftSize = estimateShuffleSize(leftShuffleId)
    val rightSize = estimateShuffleSize(rightShuffleId)
    val broadcastThreshold = ScarletConf.get.broadcastJoinThreshold

    val selectedStrategy = if (leftSize <= broadcastThreshold || rightSize <= broadcastThreshold) {
      Plan.JoinStrategy.Broadcast
    } else if (ScarletConf.get.enableSortMergeJoin) {
      Plan.JoinStrategy.SortMerge
    } else {
      Plan.JoinStrategy.ShuffleHash
    }

    logger.info(
      s"Auto-selected join strategy: $selectedStrategy (leftSize=$leftSize, rightSize=$rightSize, threshold=$broadcastThreshold)",
    )
    selectedStrategy
  }

  /**
   * Estimate the size of a shuffle dataset for join strategy selection.
   */
  private def estimateShuffleSize(shuffleId: ShuffleId): Long = {
    val numPartitions = shuffle.partitionCount(shuffleId)
    (0 until numPartitions).map { partitionId =>
      val partition = shuffle.readPartition[Any, Any](shuffleId, PartitionId(partitionId))
      partition.data.size.toLong
    }.sum
  }

  /**
   * Execute broadcast-hash join by broadcasting the smaller dataset.
   */
  @SuppressWarnings(
    Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Any"),
  )
  def executeBroadcastHashJoin(
      leftShuffleId: ShuffleId,
      rightShuffleId: ShuffleId,
      numPartitions: Int,
  ): F[Seq[Partition[_]]] = {
    val leftSize = estimateShuffleSize(leftShuffleId)
    val rightSize = estimateShuffleSize(rightShuffleId)

    if (leftSize <= rightSize) {
      // Broadcast left side, iterate over right side
      val leftData = (0 until numPartitions).flatMap { partitionId =>
        shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data
      }
      val broadcastId = ScarletRuntime.get.broadcast.broadcast(leftData)
      val broadcastData = ScarletRuntime.get.broadcast.getBroadcast[(Any, Any)](broadcastId)
      val broadcastMap =
        broadcastData.groupBy(_._1).view.map { case (k, values) => k -> values.map(_._2) }.toMap
      val tasks = (0 until numPartitions).map { partitionId =>
        val rightLocalData: Seq[(Any, Any)] =
          shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data.toSeq
        Task.BroadcastHashJoinTask[Any, Any, Any](
          rightLocalData,
          broadcastMap,
          isRightLocal = true,
        )
      }
      // Erasure boundary: every task in the submission produces the stage's record type
      scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
    } else {
      // Broadcast right side, iterate over left side
      val rightData = (0 until numPartitions).flatMap { partitionId =>
        shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data
      }
      val broadcastId = ScarletRuntime.get.broadcast.broadcast(rightData)
      val broadcastData = ScarletRuntime.get.broadcast.getBroadcast[(Any, Any)](broadcastId)
      val broadcastMap =
        broadcastData.groupBy(_._1).view.map { case (k, values) => k -> values.map(_._2) }.toMap
      val tasks = (0 until numPartitions).map { partitionId =>
        val leftLocalData: Seq[(Any, Any)] =
          shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data.toSeq
        Task.BroadcastHashJoinTask[Any, Any, Any](
          leftLocalData,
          broadcastMap,
          isRightLocal = false,
        )
      }
      // Erasure boundary: every task in the submission produces the stage's record type
      scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
    }
  }

  /**
   * Execute sort-merge join by sorting both sides before merging.
   */
  @SuppressWarnings(
    Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Any"),
  )
  def executeSortMergeJoin(
      leftShuffleId: ShuffleId,
      rightShuffleId: ShuffleId,
      numPartitions: Int,
  ): F[Seq[Partition[_]]] = {
    // For sort-merge join, we need to ensure data is sorted by key within each partition
    val tasks = (0 until numPartitions).map { partitionId =>
      val leftData: Seq[(Any, Any)] =
        shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data.toSeq
      val rightData: Seq[(Any, Any)] =
        shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data.toSeq

      // Use Any ordering for simplicity - in production would use proper key ordering
      @SuppressWarnings(Array("org.wartremover.warts.Any"))
      given Ordering[Any] = Ordering.fromLessThan((a, b) => a.hashCode() < b.hashCode())
      Task.SortMergeJoinTask[Any, Any, Any](leftData, rightData)
    }
    scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
  }

  /**
   * Execute shuffle-hash join (the original implementation).
   */
  @SuppressWarnings(
    Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Any"),
  )
  def executeShuffleHashJoin(
      leftShuffleId: ShuffleId,
      rightShuffleId: ShuffleId,
      numPartitions: Int,
  ): F[Seq[Partition[_]]] = {
    val tasks = (0 until numPartitions).map { partitionId =>
      val l: Seq[(Any, Any)] =
        shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data.toSeq
      val r: Seq[(Any, Any)] =
        shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data.toSeq
      Task.ShuffleHashJoinTask[Any, Any, Any](l, r)
    }
    scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
  }
