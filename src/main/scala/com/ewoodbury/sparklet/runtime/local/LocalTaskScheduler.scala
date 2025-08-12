package com.ewoodbury.sparklet.runtime.local

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.Partition
import com.ewoodbury.sparklet.execution.Task
import com.ewoodbury.sparklet.runtime.api.TaskScheduler

/**
 * Local implementation of the task scheduler using cats-effect IO for safe, bounded concurrency.
 *
 * Tasks are executed with a parallelism bound; each task body is run on the blocking pool using
 * IO.blocking to avoid compute pool starvation.
 */
final class LocalTaskScheduler(parallelism: Int) extends TaskScheduler[IO] with StrictLogging:

  /**
   * Submits tasks and evaluates them in parallel, respecting the configured parallelism.
   */
  def submit[A, B](tasks: Seq[Task[A, B]]): IO[Seq[Partition[B]]] =
    logger.debug(
      s"LocalTaskScheduler: submitting ${tasks.length} tasks with parallelism=$parallelism",
    )
    Semaphore[IO](parallelism.toLong).flatMap { semaphore =>
      tasks.toList
        .parTraverse { task =>
          semaphore.permit.use { _ =>
            IO.blocking(task.run())
          }
        }
        .guarantee(IO(logger.debug("LocalTaskScheduler: all tasks completed")))
    }

  /**
   * No-op for local scheduler; resources are managed by the runtime.
   */
  def shutdown(): IO[Unit] = IO.unit
