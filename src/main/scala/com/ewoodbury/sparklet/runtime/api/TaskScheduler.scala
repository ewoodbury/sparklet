package com.ewoodbury.sparklet.runtime.api

import com.ewoodbury.sparklet.core.Partition
import com.ewoodbury.sparklet.execution.Task

/**
 * Schedules tasks for execution and collects their results.
 */
trait TaskScheduler:
  /**
   * Submits a sequence of tasks (typically from a single stage) and returns results in order.
   */
  def submit[A, B](tasks: Seq[Task[A, B]]): Seq[Partition[B]]

  /**
   * Releases any resources (e.g., thread pools) associated with this scheduler.
   */
  def shutdown(): Unit

