package com.ewoodbury.sparklet.runtime.api

import cats.effect.IO

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

  // Optional per-thread override to isolate tests or specific executions
  private val threadLocal: ThreadLocal[RuntimeComponents | Null] =
    new ThreadLocal[RuntimeComponents | Null]()

  final case class RuntimeComponents(
      scheduler: TaskScheduler[IO],
      executor: ExecutorBackend,
      shuffle: ShuffleService,
      partitioner: Partitioner,
  )

  def get: RuntimeComponents = {
    Option(threadLocal.get()).getOrElse(current)
  }

  /** Sets the global runtime components (visible to all threads that don't override). */
  def set(components: RuntimeComponents): Unit = current = components

  /** Sets runtime components only for the current thread. */
  def setForCurrentThread(components: RuntimeComponents): Unit =
    threadLocal.set(components)

  /** Clears the current-thread override, falling back to the global components. */
  def clearForCurrentThread(): Unit = threadLocal.remove()

