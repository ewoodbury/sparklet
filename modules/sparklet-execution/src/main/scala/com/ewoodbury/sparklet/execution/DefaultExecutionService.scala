package com.ewoodbury.sparklet.execution

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import com.ewoodbury.sparklet.core.{ExecutionService, Plan}
import com.ewoodbury.sparklet.runtime.SparkletRuntime

/**
 * Implementation of ExecutionService that bridges the API to the execution engine.
 */
class DefaultExecutionService extends ExecutionService {

  def execute[A](plan: Plan[A]): Seq[A] = {
    if (DAGScheduler.requiresDAGScheduling(plan)) {
      // Use DAG scheduler for wide transformations
      val rt = SparkletRuntime.get
      val scheduler = new DAGScheduler[IO](rt.shuffle, rt.scheduler, rt.partitioner)
      scheduler.execute(plan).unsafeRunSync().toSeq
    } else {
      // Use single-stage execution for narrow transformations
      plan match {
        case s: Plan.Source[A] =>
          // Sources don't need tasks, just return the data directly
          s.partitions.flatMap(_.data)

        case _ =>
          val tasks = Executor.createTasks(plan)
          // Cast to the expected type for TaskScheduler - this is safe because createTasks
          // returns tasks that produce the correct output type A
          @SuppressWarnings(Array("org.wartremover.warts.Any"))
          val typedTasks = tasks.asInstanceOf[Seq[Task[Any, A]]]
          @SuppressWarnings(Array("org.wartremover.warts.Any"))
          val resultPartitions = SparkletRuntime.get.scheduler.submit(typedTasks).unsafeRunSync()
          resultPartitions.flatMap(_.data)
      }
    }
  }

  def count[A](plan: Plan[A]): Long = {
    execute(plan).size.toLong
  }

  def take[A](plan: Plan[A], n: Int): Seq[A] = {
    // This is inefficient as it executes the full plan. A real implementation
    // would have optimized execution for take().
    execute(plan).take(n)
  }
}

/**
 * Auto-registration of the default execution service.
 * This gets called when the execution module is loaded.
 */
object DefaultExecutionService {
  // Register the service when this object is first accessed
  private lazy val registration = {
    com.ewoodbury.sparklet.core.ExecutionService.set(new DefaultExecutionService())
  }

  // Ensure the registration happens by calling this method
  def initialize(): Unit = {
    // Force the lazy initialization to run
    registration
  }
}
