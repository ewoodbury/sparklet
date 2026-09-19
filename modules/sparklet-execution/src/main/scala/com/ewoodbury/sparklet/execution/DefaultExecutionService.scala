package com.ewoodbury.sparklet.execution

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import com.ewoodbury.sparklet.core.{ExecutionService, Partition, Plan}
import com.ewoodbury.sparklet.runtime.SparkletRuntime

/**
 * Implementation of ExecutionService that bridges the API to the execution engine. Every plan is
 * compiled into a stage graph and executed through the DAGScheduler; narrow plans simply produce
 * one-stage graphs.
 */
class DefaultExecutionService extends ExecutionService {

  def execute[A](plan: Plan[A]): Seq[A] =
    executePartitions(plan).flatMap(_.data)

  def executePartitions[A](plan: Plan[A]): Seq[Partition[A]] = plan match {
    case s: Plan.Source[A] =>
      // A bare source needs no tasks; its partitions are the result
      s.partitions

    case _ =>
      val rt = SparkletRuntime.get
      val scheduler = new DAGScheduler[IO](rt.shuffle, rt.scheduler, rt.partitioner)
      scheduler.executePartitions(plan).unsafeRunSync()
  }

  def count[A](plan: Plan[A]): Long =
    execute(plan).size.toLong

  def take[A](plan: Plan[A], n: Int): Seq[A] =
    if (n <= 0) Seq.empty
    else {
      // Truncate each output partition of the final stage to n elements before executing. Each
      // partition can then contribute at most n elements, so the first n elements of the
      // partition-ordered result are unchanged.
      val limited = Plan.MapPartitionsOp(plan, (it: Iterator[A]) => it.take(n))
      executePartitions(limited).flatMap(_.data).take(n)
    }
}

/**
 * Auto-registration of the default execution service. This gets called when the execution module
 * is loaded.
 */
object DefaultExecutionService {
  // Register the service when this object is first accessed
  private lazy val registration = {
    ExecutionService.set(new DefaultExecutionService())
  }

  // Ensure the registration happens by calling this method
  def initialize(): Unit = {
    // Force the lazy initialization to run
    registration
  }
}
