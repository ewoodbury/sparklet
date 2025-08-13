package com.ewoodbury.sparklet.runtime.local

import com.ewoodbury.sparklet.core.Partition
import com.ewoodbury.sparklet.runtime.api.{ExecutorBackend, RunnableTask}

final class LocalExecutorBackend extends ExecutorBackend:
  def run[A, B](task: RunnableTask[A, B]): Partition[B] = task.run()


