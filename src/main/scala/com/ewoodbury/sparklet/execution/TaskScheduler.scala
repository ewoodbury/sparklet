package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.Partition
import com.ewoodbury.sparklet.runtime.api.SparkletRuntime

object TaskScheduler:
  /**
   * Shim that delegates to the active runtime's TaskScheduler SPI.
   */
  def submit[A, B](tasks: Seq[Task[A, B]]): Seq[Partition[B]] =
    SparkletRuntime.get.scheduler.submit(tasks)

  def shutdown(): Unit =
    SparkletRuntime.get.scheduler.shutdown()

