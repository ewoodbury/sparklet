package com.ewoodbury.sparklet.runtime.api

import com.ewoodbury.sparklet.core.SparkletConf
import com.ewoodbury.sparklet.runtime.local.{HashPartitioner, LocalExecutorBackend, LocalShuffleService, LocalTaskScheduler}

/**
 * Global wiring holder for the active Sparklet runtime (scheduler, shuffle, etc.).
 */
object SparkletRuntime:
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var current: RuntimeComponents =
    RuntimeComponents(
      scheduler = new LocalTaskScheduler(SparkletConf.get.threadPoolSize),
      executor = new LocalExecutorBackend,
      shuffle = new LocalShuffleService,
      partitioner = new HashPartitioner,
    )

  final case class RuntimeComponents(
      scheduler: TaskScheduler,
      executor: ExecutorBackend,
      shuffle: ShuffleService,
      partitioner: Partitioner,
  )

  def get: RuntimeComponents = current
  def set(components: RuntimeComponents): Unit = current = components

