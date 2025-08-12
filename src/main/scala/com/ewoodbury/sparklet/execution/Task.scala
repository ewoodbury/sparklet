package com.ewoodbury.sparklet.execution

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.{Logger, StrictLogging}

import com.ewoodbury.sparklet.core.{Partition, Plan}
import com.ewoodbury.sparklet.runtime.api.SparkletRuntime

/**
 * A Task represents a unit of computation that can be run on an executor. It operates on a single
 * input partition to produce a single output partition.
 */
sealed trait Task[A, B]:
  def partition: Partition[A]
  def run(): Partition[B] // The execution logic

object Task extends StrictLogging:
  private val taskLogger: Logger = logger

  /** A task that applies a map function to a partition. */
  case class MapTask[A, B](
      partition: Partition[A],
      f: A => B,
  ) extends Task[A, B]:
    override def run(): Partition[B] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] MapTask on partition")
      Partition(partition.data.map(f))
    }

  /** A task that applies a filter function to a partition. */
  case class FilterTask[A](
      partition: Partition[A],
      p: A => Boolean,
  ) extends Task[A, A]:
    override def run(): Partition[A] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] FilterTask on partition")
      Partition(partition.data.filter(p))
    }

  /** A task that applies a flatMap function to a partition. */
  case class FlatMapTask[A, B](
      partition: Partition[A],
      f: A => IterableOnce[B],
  ) extends Task[A, B]:
    override def run(): Partition[B] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] FlatMapTask on partition")
      Partition(partition.data.flatMap(f))
    }

  /** A task that applies a distinct function to a partition. */
  case class DistinctTask[A](
      partition: Partition[A],
  ) extends Task[A, A]:
    override def run(): Partition[A] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] DistinctTask on partition")
      Partition(partition.data.toSeq.distinct)
    }

  /** A task that applies a keys function to a partition. */
  case class KeysTask[K, V](
      partition: Partition[(K, V)],
  ) extends Task[(K, V), K]:
    override def run(): Partition[K] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] KeysTask on partition")
      Partition(partition.data.map(_._1))
    }

  /** A task that applies a values function to a partition. */
  case class ValuesTask[K, V](
      partition: Partition[(K, V)],
  ) extends Task[(K, V), V]:
    override def run(): Partition[V] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] ValuesTask on partition")
      Partition(partition.data.map(_._2))
    }

  /** A task that applies a mapValues function to a partition. */
  case class MapValuesTask[K, V, B](
      partition: Partition[(K, V)],
      f: V => B,
  ) extends Task[(K, V), (K, B)]:
    override def run(): Partition[(K, B)] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] MapValuesTask on partition")
      Partition(partition.data.map { case (k, v) => (k, f(v)) })
    }

  /** A task that applies a filterKeys function to a partition. */
  case class FilterKeysTask[K, V](
      partition: Partition[(K, V)],
      p: K => Boolean,
  ) extends Task[(K, V), (K, V)]:
    override def run(): Partition[(K, V)] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] FilterKeysTask on partition")
      Partition(partition.data.filter { case (k, _) => p(k) })
    }

  /** A task that applies a filterValues function to a partition. */
  case class FilterValuesTask[K, V](
      partition: Partition[(K, V)],
      p: V => Boolean,
  ) extends Task[(K, V), (K, V)]:
    override def run(): Partition[(K, V)] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] FilterValuesTask on partition")
      Partition(partition.data.filter { case (_, v) => p(v) })
    }

  /** A task that applies a flatMapValues function to a partition. */
  case class FlatMapValuesTask[K, V, B](
      partition: Partition[(K, V)],
      f: V => IterableOnce[B],
  ) extends Task[(K, V), (K, B)]:
    override def run(): Partition[(K, B)] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] FlatMapValuesTask on partition")
      Partition(partition.data.flatMap { case (k, v) => f(v).iterator.map(b => (k, b)) })
    }

  /** A task that executes a complete stage (chain of narrow transformations) on a partition. */
  case class StageTask[A, B](
      partition: Partition[A],
      stage: Stage[A, B],
  ) extends Task[A, B]:
    override def run(): Partition[B] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] StageTask on partition")
      stage.execute(partition)
    }

  /** A task that executes a complete DAG through the DAGScheduler. */
  case class DAGTask[A](plan: Plan[A]) extends Task[A, A]:
    // DAGTask doesn't operate on a single partition, but the Task trait requires this
    // We use an empty partition as a placeholder since DAGTask orchestrates entire DAG execution
    override def partition: Partition[A] = Partition(Seq.empty[A])

    override def run(): Partition[A] = {
      taskLogger.debug(s"[${Thread.currentThread().getName}] DAGTask executing DAG")
      val rt = SparkletRuntime.get
      val scheduler = new DAGScheduler[IO](rt.shuffle, rt.scheduler, rt.partitioner)
      val results = scheduler.execute(plan).unsafeRunSync()
      Partition(results.toSeq)
    }

end Task
