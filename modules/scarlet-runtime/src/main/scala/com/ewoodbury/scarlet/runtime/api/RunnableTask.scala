package com.ewoodbury.scarlet.runtime.api

import com.ewoodbury.scarlet.core.Partition

/**
 * A minimal task interface that runtime schedulers and executors can run without depending on the
 * concrete execution implementation module.
 */
trait RunnableTask[A, B]:
  def run(): Partition[B]
