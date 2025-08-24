package com.ewoodbury.sparklet.execution

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.{Logger, StrictLogging}

import com.ewoodbury.sparklet.core.{Partition, Plan, ShuffleId, LineageInfo, TaskResult, TaskSuccess, TaskFailure, StageId}
import com.ewoodbury.sparklet.runtime.SparkletRuntime
import com.ewoodbury.sparklet.runtime.api.RunnableTask



/**
 * A Task represents a unit of computation that can be run on an executor. It operates on a single
 * input partition to produce a single output partition.
 */
sealed trait Task[A, B] extends RunnableTask[A, B]:
  def partition: Partition[A]
  def lineage: Option[LineageInfo] = None // Optional lineage for backward compatibility
  def run(): Partition[B] // The execution logic
  def runWithLineage(): TaskResult[B] = // Default implementation for backward compatibility
    try {
      val result = run()
      lineage match {
        case Some(lin) => TaskSuccess(result, lin.copy(attemptCount = lin.attemptCount + 1))
        case None => TaskSuccess(result, LineageInfo(StageId(0), 0, Seq.empty, Seq.empty, "unknown"))
      }
    } catch {
      case e: Exception =>
        val lin = lineage.getOrElse(LineageInfo(StageId(0), 0, Seq.empty, Seq.empty, "unknown"))
        TaskFailure(lin, e, 1)
    }

object Task extends StrictLogging:
  private val taskLogger: Logger = logger

  /** A task that applies a map function to a partition. */
  case class MapTask[A, B](
      partition: Partition[A],
      f: A => B,
      override val lineage: Option[LineageInfo] = None
  ) extends Task[A, B]:
    override def run(): Partition[B] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] MapTask on partition")
      val it = partition.data.iterator.map(f)
      Partition(IterUtil.iterableOf(it))
    }

  /** A task that applies a filter function to a partition. */
  case class FilterTask[A](
      partition: Partition[A],
      p: A => Boolean,
      override val lineage: Option[LineageInfo] = None
  ) extends Task[A, A]:
    override def run(): Partition[A] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] FilterTask on partition")
      val it = partition.data.iterator.filter(p)
      Partition(IterUtil.iterableOf(it))
    }

  /** A task that applies a flatMap function to a partition. */
  case class FlatMapTask[A, B](
      partition: Partition[A],
      f: A => IterableOnce[B],
      override val lineage: Option[LineageInfo] = None
  ) extends Task[A, B]:
    override def run(): Partition[B] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] FlatMapTask on partition")
      val it = partition.data.iterator.flatMap(a => f(a).iterator)
      Partition(IterUtil.iterableOf(it))
    }

  /** A task that applies a distinct function to a partition. */
  case class DistinctTask[A](
      partition: Partition[A],
      override val lineage: Option[LineageInfo] = None
  ) extends Task[A, A]:
    override def run(): Partition[A] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] DistinctTask on partition")
      val it = partition.data.iterator.distinct
      Partition(IterUtil.iterableOf(it))
    }

  /** A task that applies a keys function to a partition. */
  case class KeysTask[K, V](
      partition: Partition[(K, V)],
      override val lineage: Option[LineageInfo] = None
  ) extends Task[(K, V), K]:
    override def run(): Partition[K] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] KeysTask on partition")
      val it = partition.data.iterator.map(_._1)
      Partition(IterUtil.iterableOf(it))
    }

  /** A task that applies a values function to a partition. */
  case class ValuesTask[K, V](
      partition: Partition[(K, V)],
      override val lineage: Option[LineageInfo] = None
  ) extends Task[(K, V), V]:
    override def run(): Partition[V] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] ValuesTask on partition")
      val it = partition.data.iterator.map(_._2)
      Partition(IterUtil.iterableOf(it))
    }

  /** A task that applies a mapValues function to a partition. */
  case class MapValuesTask[K, V, B](
      partition: Partition[(K, V)],
      f: V => B,
      override val lineage: Option[LineageInfo] = None
  ) extends Task[(K, V), (K, B)]:
    override def run(): Partition[(K, B)] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] MapValuesTask on partition")
      val it = partition.data.iterator.map { case (k, v) => (k, f(v)) }
      Partition(IterUtil.iterableOf(it))
    }

  /** A task that applies a filterKeys function to a partition. */
  case class FilterKeysTask[K, V](
      partition: Partition[(K, V)],
      p: K => Boolean,
      override val lineage: Option[LineageInfo] = None
  ) extends Task[(K, V), (K, V)]:
    override def run(): Partition[(K, V)] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] FilterKeysTask on partition")
      val it = partition.data.iterator.filter { case (k, _) => p(k) }
      Partition(IterUtil.iterableOf(it))
    }

  /** A task that applies a filterValues function to a partition. */
  case class FilterValuesTask[K, V](
      partition: Partition[(K, V)],
      p: V => Boolean,
      override val lineage: Option[LineageInfo] = None
  ) extends Task[(K, V), (K, V)]:
    override def run(): Partition[(K, V)] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] FilterValuesTask on partition")
      val it = partition.data.iterator.filter { case (_, v) => p(v) }
      Partition(IterUtil.iterableOf(it))
    }

  /** A task that applies a flatMapValues function to a partition. */
  case class FlatMapValuesTask[K, V, B](
      partition: Partition[(K, V)],
      f: V => IterableOnce[B],
      override val lineage: Option[LineageInfo] = None
  ) extends Task[(K, V), (K, B)]:
    override def run(): Partition[(K, B)] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] FlatMapValuesTask on partition")
      val it = partition.data.iterator.flatMap { case (k, v) => f(v).iterator.map(b => (k, b)) }
      Partition(IterUtil.iterableOf(it))
    }

  /** A task that executes a complete stage (chain of narrow transformations) on a partition. */
  case class StageTask[A, B](
      partition: Partition[A],
      stage: Stage[A, B],
      override val lineage: Option[LineageInfo] = None
  ) extends Task[A, B]:
    override def run(): Partition[B] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] StageTask on partition")
      stage.execute(partition)
    }

  /** A task that executes a complete DAG through the DAGScheduler. */
  case class DAGTask[A](
      plan: Plan[A],
      override val lineage: Option[LineageInfo] = None
  ) extends Task[A, A]:
    // DAGTask doesn't operate on a single partition, but the Task trait requires this
    // We use an empty partition as a placeholder since DAGTask orchestrates entire DAG execution
    override def partition: Partition[A] = Partition(Seq.empty[A])

    override def run(): Partition[A] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] DAGTask executing DAG")
      val rt = SparkletRuntime.get
      val scheduler = new DAGScheduler[IO](rt.shuffle, rt.scheduler, rt.partitioner)
      val results = scheduler.execute(plan).unsafeRunSync()
      // Keep Seq materialization for DAGTask to satisfy tests that expect Seq-typed data
      // TODO: Remove toSeq and use iterator, and remove Seq checks from tests.
      Partition(results.toSeq)
    }

  /** A per-partition shuffle-hash inner join task that joins two co-partitioned inputs. */
  final case class ShuffleHashJoinTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)]
  ) extends RunnableTask[Any, (K, (L, R))]:
    override def run(): Partition[(K, (L, R))] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] ShuffleHashJoinTask on partition")
      // Build a hash map from the smaller side to reduce memory and CPU
      val (small, large, emitLeftFirst) =
        if (leftData.size <= rightData.size) (leftData, rightData, true)
        else (rightData, leftData, false)
      val grouped = small.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
      val outIter = large.iterator.flatMap { case (k, v) =>
        grouped.getOrElse(k, Seq.empty[L]).iterator.map { s =>
          if (emitLeftFirst) (k, (s.asInstanceOf[L], v.asInstanceOf[R]))
          else (k, (v.asInstanceOf[L], s.asInstanceOf[R]))
        }
      }
      Partition(IterUtil.iterableOf(outIter))
    }

  /** A per-partition sort-merge inner join task that joins two sorted, co-partitioned inputs. */
  final case class SortMergeJoinTask[K, L, R](
      leftData: Seq[(K, L)],
      rightData: Seq[(K, R)]
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

  /** A broadcast-hash inner join task that joins a local partition with a broadcast dataset. */
  final case class BroadcastHashJoinTask[K, L, R](
      localData: Seq[(K, L)],
      broadcastMap: Map[K, Seq[R]],
      isRightLocal: Boolean
  ) extends RunnableTask[Any, (K, (L, R))]:
    override def run(): Partition[(K, (L, R))] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] BroadcastHashJoinTask on partition")
      val result = localData.iterator.flatMap { case (k, localValue) =>
        broadcastMap.getOrElse(k, Seq.empty[R]).iterator.map { broadcastValue =>
          if (isRightLocal) {
            // Local data is right side, broadcast is left side
            (k, (broadcastValue.asInstanceOf[L], localValue.asInstanceOf[R]))
          } else {
            // Local data is left side, broadcast is right side
            (k, (localValue, broadcastValue))
          }
        }
      }
      Partition(IterUtil.iterableOf(result))
    }

end Task
