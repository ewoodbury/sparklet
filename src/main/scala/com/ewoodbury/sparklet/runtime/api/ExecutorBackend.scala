package com.ewoodbury.sparklet.runtime.api

import com.ewoodbury.sparklet.core.Partition
import com.ewoodbury.sparklet.execution.Task

/**
 * Abstraction over how an individual task is executed (e.g., local thread, remote worker).
 */
trait ExecutorBackend:
  /**
   * Execute a single task and return its output partition.
   */
  def run[A, B](task: Task[A, B]): Partition[B]



