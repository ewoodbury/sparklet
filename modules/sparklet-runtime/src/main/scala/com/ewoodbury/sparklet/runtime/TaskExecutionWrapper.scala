package com.ewoodbury.sparklet.runtime

import cats.effect.Async
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.{Partition, RetryPolicy}
import com.ewoodbury.sparklet.runtime.api.RunnableTask

/**
 * Wrapper that executes tasks with retry logic. Handles transient failures with exponential
 * backoff and propagates permanent failures to the caller.
 */
class TaskExecutionWrapper[F[_]: Async] private (
    retryPolicy: RetryPolicy,
) extends StrictLogging {

  /**
   * Execute a task with retry logic.
   *
   * @param task
   *   The task to execute
   * @param taskId
   *   Identifier for the task used in logging
   * @tparam A
   *   Input type
   * @tparam B
   *   Output type
   * @return
   *   Effect that yields the partition result or throws exception
   */
  def executeWithRetry[A, B](
      task: RunnableTask[A, B],
      taskId: String = "unknown",
  ): F[Partition[B]] = {
    executeWithRetryInternal(task, taskId, attempt = 1)
  }

  private def executeWithRetryInternal[A, B](
      task: RunnableTask[A, B],
      taskId: String,
      attempt: Int,
  ): F[Partition[B]] = {
    logger.debug(s"Executing task $taskId (attempt $attempt)")

    // Execute the task in a blocking context
    Async[F].blocking(task.run()).attempt.flatMap {
      case Right(partition) =>
        logger.debug(s"Task $taskId succeeded on attempt $attempt")
        Async[F].pure(partition)

      case Left(exception) =>
        logger.warn(s"Task $taskId failed on attempt $attempt", exception)

        // Check if we should retry based on the retry policy
        if (retryPolicy.shouldRetry(attempt, exception)) {
          val delay = retryPolicy.delayBeforeRetry(attempt)

          logger.info(
            s"Retrying task $taskId in ${delay.toMillis}ms " +
              s"(attempt ${attempt + 1}/${retryPolicy.maxRetries + 1})",
          )

          // Wait for the retry delay, then retry
          Async[F].sleep(delay) *> executeWithRetryInternal(task, taskId, attempt + 1)
        } else {
          logger.error(s"Task $taskId failed permanently after $attempt attempts")
          Async[F].raiseError(exception)
        }
    }
  }
}

object TaskExecutionWrapper {

  /**
   * Create a task execution wrapper with the given retry policy.
   */
  def withRetryPolicy[F[_]: Async](retryPolicy: RetryPolicy): TaskExecutionWrapper[F] =
    new TaskExecutionWrapper[F](retryPolicy)

  /**
   * Create a default TaskExecutionWrapper using the default retry policy.
   */
  def default[F[_]: Async]: TaskExecutionWrapper[F] =
    withRetryPolicy(RetryPolicy.default)

  /**
   * Create a TaskExecutionWrapper with no retries.
   */
  def noRetries[F[_]: Async]: TaskExecutionWrapper[F] =
    withRetryPolicy(RetryPolicy.NoRetry)
}
