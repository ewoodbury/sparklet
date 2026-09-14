package com.ewoodbury.sparklet.runtime.local

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.{Partition, RetryPolicy, SparkletConf}
import com.ewoodbury.sparklet.runtime.TaskExecutionWrapper
import com.ewoodbury.sparklet.runtime.api.{RunnableTask, TaskScheduler}

/**
 * Local implementation of the task scheduler.
 *
 * Tasks are executed with a parallelism bound; each task body is run on the blocking pool using
 * IO.blocking to avoid compute pool starvation. Normal submission applies the retry policy derived
 * from [[SparkletConf]]; submission with an explicit policy override is available through
 * `submitWithRetry`.
 */
final class LocalTaskScheduler(
    parallelism: Int,
    retryPolicy: RetryPolicy = SparkletConf.get.retryPolicy,
) extends TaskScheduler[IO]
    with StrictLogging:

  private val executionWrapper: TaskExecutionWrapper[IO] =
    TaskExecutionWrapper.withRetryPolicy[IO](retryPolicy)

  /**
   * Submits tasks and evaluates them in parallel, respecting the configured parallelism. Failed
   * tasks are retried according to the configured retry policy.
   */
  def submit[A, B](tasks: Seq[RunnableTask[A, B]]): IO[Seq[Partition[B]]] = {
    logger.debug(
      s"LocalTaskScheduler: submitting ${tasks.length} tasks with parallelism=$parallelism " +
        s"and maxRetries=${retryPolicy.maxRetries}",
    )
    executeWithWrapper(tasks, executionWrapper)
      .guarantee(IO(logger.debug("LocalTaskScheduler: all tasks completed")))
  }

  /**
   * Submits tasks with an explicit retry policy that overrides the configured one.
   */
  def submitWithRetry[A, B](
      tasks: Seq[RunnableTask[A, B]],
      policy: RetryPolicy,
  ): IO[Seq[Partition[B]]] = {
    logger.debug(
      s"LocalTaskScheduler: submitting ${tasks.length} tasks with explicit retry policy (maxRetries=${policy.maxRetries})",
    )
    executeWithWrapper(tasks, TaskExecutionWrapper.withRetryPolicy[IO](policy))
  }

  /**
   * Execute tasks with the given wrapper, preserving input order in the results and bounding
   * concurrency at `parallelism`.
   */
  private def executeWithWrapper[A, B](
      tasks: Seq[RunnableTask[A, B]],
      wrapper: TaskExecutionWrapper[IO],
  ): IO[Seq[Partition[B]]] =
    Semaphore[IO](parallelism.toLong).flatMap { semaphore =>
      tasks.toList.parTraverse { task =>
        semaphore.permit.use { _ =>
          wrapper.executeWithRetry(task)
        }
      }
    }

  /**
   * No-op for local scheduler; resources are managed by the runtime.
   */
  def shutdown(): IO[Unit] = IO.unit
