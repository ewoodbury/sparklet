package com.ewoodbury.sparklet.runtime.local

import com.ewoodbury.sparklet.core.Partition
import com.ewoodbury.sparklet.execution.Task
import com.ewoodbury.sparklet.runtime.api.ExecutorBackend

final class LocalExecutorBackend extends ExecutorBackend:
  def run[A, B](task: Task[A, B]): Partition[B] = task.run()

