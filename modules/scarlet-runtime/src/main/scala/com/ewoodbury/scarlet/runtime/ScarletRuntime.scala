package com.ewoodbury.scarlet.runtime

import cats.effect.IO

import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.api.*
import com.ewoodbury.scarlet.runtime.local.*

/**
 * Global wiring holder for the active Scarlet runtime (scheduler, shuffle, etc.).
 */
object ScarletRuntime:
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var current: RuntimeComponents =
    RuntimeComponents(
      scheduler = new LocalTaskScheduler(ScarletConf.get.threadPoolSize),
      shuffle = new LocalShuffleService,
      partitioner = new HashPartitioner,
      broadcast = new LocalBroadcastService,
    )

  // Optional per-thread override to isolate tests or specific executions
  private val threadLocal: ThreadLocal[RuntimeComponents | Null] =
    new ThreadLocal[RuntimeComponents | Null]()

  final case class RuntimeComponents(
      scheduler: TaskScheduler[IO],
      shuffle: ShuffleService,
      partitioner: Partitioner,
      broadcast: BroadcastService,
  )

  def get: RuntimeComponents = {
    Option(threadLocal.get()).getOrElse(current)
  }

  /** Sets the global runtime components (visible to all threads that don't override). */
  def set(components: RuntimeComponents): Unit = current = components

  /** Sets runtime components only for the current thread. */
  def setForCurrentThread(components: RuntimeComponents): Unit =
    threadLocal.set(components)

  /** Clears the thread-local override (restores global components). */
  def clearThreadLocal(): Unit = threadLocal.remove()
