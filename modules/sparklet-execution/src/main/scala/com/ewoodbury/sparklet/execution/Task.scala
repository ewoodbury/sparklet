package com.ewoodbury.sparklet.execution

import com.typesafe.scalalogging.{Logger, StrictLogging}

import com.ewoodbury.sparklet.core.Partition
import com.ewoodbury.sparklet.runtime.api.RunnableTask

/**
 * A Task represents a unit of computation that can be run on an executor. It operates on a single
 * input partition to produce a single output partition.
 */
sealed trait Task[A, B] extends RunnableTask[A, B]:
  def partition: Partition[A]
  def run(): Partition[B]

object Task extends StrictLogging:
  private val taskLogger: Logger = logger

  /** Creates a StageTask from a partition and stage. */
  def createStageTask[A, B](partition: Partition[A], stage: Stage[A, B]): StageTask[A, B] =
    StageTask(partition, stage)

  /** Creates a shuffle hash join task. */
  def createShuffleHashJoinTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  ): ShuffleHashJoinTask[K, L, R] = ShuffleHashJoinTask(leftData, rightData)

  /** Creates a broadcast hash join task. */
  def createBroadcastHashJoinTask[K, L, R](
      localData: Seq[(K, L)],
      broadcastMap: Map[K, Seq[R]],
      isRightLocal: Boolean = false,
  ): BroadcastHashJoinTask[K, L, R] = BroadcastHashJoinTask(localData, broadcastMap, isRightLocal)

  /** Creates a sort merge join task with proper ordering. */
  def createSortMergeJoinTask[K: Ordering, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  ): SortMergeJoinTask[K, L, R] = SortMergeJoinTask(leftData, rightData)

  /** A task that executes a complete stage (chain of narrow transformations) on a partition. */
  case class StageTask[A, B](
      partition: Partition[A],
      stage: Stage[A, B],
  ) extends Task[A, B]:
    override def run(): Partition[B] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] StageTask on partition")
      stage.execute(partition)
    }

  /** A per-partition shuffle-hash inner join task that joins two co-partitioned inputs. */
  final case class ShuffleHashJoinTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  ) extends RunnableTask[Any, (K, (L, R))]:
    override def run(): Partition[(K, (L, R))] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] ShuffleHashJoinTask on partition")

      // Build hash maps for both sides to avoid unsafe casting
      val leftGrouped = leftData.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
      val rightGrouped = rightData.groupBy(_._1).view.mapValues(_.map(_._2)).toMap

      // Find all common keys and perform cross product
      val commonKeys = leftGrouped.keySet.intersect(rightGrouped.keySet)
      val result = for {
        key <- commonKeys.toSeq
        leftValue <- leftGrouped(key)
        rightValue <- rightGrouped(key)
      } yield (key, (leftValue, rightValue))

      Partition(result)
    }

  /** A per-partition sort-merge inner join task that joins two sorted, co-partitioned inputs. */
  final case class SortMergeJoinTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)],
  )(using keyOrdering: Ordering[K])
      extends RunnableTask[Any, (K, (L, R))]:
    override def run(): Partition[(K, (L, R))] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] SortMergeJoinTask on partition")

      // Sort both sides by key if they're not already sorted
      val sortedLeft = leftData.sortBy(_._1)
      val sortedRight = rightData.sortBy(_._1)

      // Group by key and perform cross product for matching keys
      val leftByKey = sortedLeft.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
      val rightByKey = sortedRight.groupBy(_._1).view.mapValues(_.map(_._2)).toMap

      val result = for {
        (key, leftValues) <- leftByKey.toSeq
        rightValues <- rightByKey.get(key).toSeq
        leftValue <- leftValues
        rightValue <- rightValues
      } yield (key, (leftValue, rightValue))

      Partition(result)
    }

  /**
   * A broadcast-hash inner join task that joins a local partition with a broadcast dataset.
   *
   * Named erasure boundary: the task operates on erased (Any, Any) records because join sides
   * travel through the shuffle service; when the broadcast side is the join's left input the
   * values are re-typed at the tuple level. Safe because the caller broadcasts and reads the same
   * side consistently for a given join stage.
   */
  final case class BroadcastHashJoinTask[K, L, R](
      localData: Seq[(K, L)],
      broadcastMap: Map[K, Seq[R]],
      isRightLocal: Boolean,
  ) extends RunnableTask[Any, (K, (L, R))]:
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    override def run(): Partition[(K, (L, R))] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] BroadcastHashJoinTask on partition")
      val result = localData.iterator.flatMap { case (k, localValue) =>
        broadcastMap.getOrElse(k, Seq.empty[R]).iterator.map { broadcastValue =>
          if (isRightLocal) {
            // Local data is right side (R), broadcast is left side (L)
            // This is a type system limitation - we need the cast here
            (k, (broadcastValue.asInstanceOf[L], localValue.asInstanceOf[R]))
          } else {
            // Local data is left side (L), broadcast is right side (R) - this is the normal case
            (k, (localValue, broadcastValue))
          }
        }
      }
      Partition(IterUtil.iterableOf(result))
    }

end Task
