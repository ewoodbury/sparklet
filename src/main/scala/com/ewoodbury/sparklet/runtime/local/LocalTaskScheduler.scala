package com.ewoodbury.sparklet.runtime.local

import java.util.concurrent.Executors

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.Partition
import com.ewoodbury.sparklet.execution.Task
import com.ewoodbury.sparklet.runtime.api.TaskScheduler

final class LocalTaskScheduler(threadPoolSize: Int) extends TaskScheduler with StrictLogging:
  private val executorService = Executors.newFixedThreadPool(threadPoolSize)
  private implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executorService)

  def submit[A, B](tasks: Seq[Task[A, B]]): Seq[Partition[B]] =
    logger.debug(s"LocalTaskScheduler: submitting ${tasks.length} tasks to the thread pool")
    val futures: Seq[Future[Partition[B]]] = tasks.map { task =>
      Future(task.run())
    }
    val all: Future[Seq[Partition[B]]] = Future.sequence(futures)
    val result = Await.result(all, 1.minute)
    logger.debug("LocalTaskScheduler: all tasks completed")
    result

  def shutdown(): Unit = executorService.shutdown()

