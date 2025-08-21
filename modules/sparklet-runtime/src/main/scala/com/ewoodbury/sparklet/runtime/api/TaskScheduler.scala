package com.ewoodbury.sparklet.runtime.api

import com.ewoodbury.sparklet.core.{Partition, RetryPolicy}

/**
 * Schedules tasks for execution and collects their results, within an abstract effect `F[_]`.
 */
trait TaskScheduler[F[_]]:
  /**
   * Submits a sequence of tasks (typically from a single stage) and returns results in order.
   *
   * @param tasks
   *   The tasks to execute. All tasks are assumed to be independent and can be executed in
   *   parallel with bounded concurrency depending on the scheduler implementation.
   * @tparam A
   *   Input element type for each task's partition.
   * @tparam B
   *   Output element type produced by each task.
   * @return
   *   An effect that yields the sequence of output partitions in the same order as the input
   *   tasks.
   */
  def submit[A, B](tasks: Seq[RunnableTask[A, B]]): F[Seq[Partition[B]]]

  /**
   * Submits tasks with retry logic using the provided retry policy.
   *
   * @param tasks The tasks to execute with retry support
   * @param retryPolicy The policy determining retry behavior and delays
   * @tparam A Input element type for each task's partition
   * @tparam B Output element type produced by each task
   * @return An effect that yields the sequence of output partitions
   */
  def submitWithRetry[A, B](
    tasks: Seq[RunnableTask[A, B]],
    retryPolicy: RetryPolicy
  ): F[Seq[Partition[B]]]

  /**
   * Releases any resources (e.g., thread pools) associated with this scheduler.
   */
  def shutdown(): F[Unit]
