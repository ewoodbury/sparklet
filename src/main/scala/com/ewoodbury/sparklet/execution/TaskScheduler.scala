package com.ewoodbury.sparklet.execution

import java.util.concurrent.Executors

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

import com.ewoodbury.sparklet.core.Partition

object TaskScheduler:
  // Thread pool with 4 "executors" (cores)
  private val executorService = Executors.newFixedThreadPool(4)
  private implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(executorService)

  /**
   * Submits a set of tasks (representing one stage) for concurrent execution.
   *
   * @param tasks
   *   The sequence of tasks to run.
   * @return
   *   The sequence of computed Partitions, in order.
   */
  def submit[A, B](tasks: Seq[Task[A, B]]): Seq[Partition[B]] = {
    println(s"TaskScheduler: Submitting ${tasks.length} tasks to the thread pool...")

    // For each task, create a Future that will run it on our thread pool
    val futures: Seq[Future[Partition[B]]] = tasks.map { task =>
      Future {
        task.run()
      }
    }

    // `Future.sequence` turns a Seq[Future[T]] into a Future[Seq[T]].
    // It waits for all futures to complete.
    val allResultsFuture: Future[Seq[Partition[B]]] = Future.sequence(futures)

    // Block and wait for the final result.
    // In a real system, this would be handled asynchronously.
    val result = Await.result(allResultsFuture, 1.minute) // Wait up to 1 minute
    println("TaskScheduler: All tasks completed.")
    result
  }

  def shutdown(): Unit = {
    executorService.shutdown()
  }
