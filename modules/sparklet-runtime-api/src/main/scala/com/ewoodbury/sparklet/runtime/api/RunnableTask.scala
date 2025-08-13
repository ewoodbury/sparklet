package com.ewoodbury.sparklet.runtime.api

import com.ewoodbury.sparklet.core.Partition

/**
  * A minimal task interface that runtime schedulers and executors can run without depending on the
  * concrete execution implementation module.
  */
trait RunnableTask[A, B]:
  def run(): Partition[B]


