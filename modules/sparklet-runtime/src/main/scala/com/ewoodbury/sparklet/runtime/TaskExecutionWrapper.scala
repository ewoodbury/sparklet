package com.ewoodbury.sparklet.runtime

import cats.effect.{Async, Sync}
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.{Partition, RetryPolicy}
import com.ewoodbury.sparklet.runtime.api.RunnableTask

import scala.concurrent.duration.*
import scala.util.control.NonFatal

/**
 * Wrapper that executes tasks with retry logic and lineage tracking.
 * Handles transient failures and provides detailed execution information.
 */
class TaskExecutionWrapper[F[_]: Async] private (
  retryPolicy: RetryPolicy,
  recoveryManager: Option[LineageRecoveryManager[F]]
) extends StrictLogging {

  /**
   * Execute a task with retry logic and lineage tracking.
   * For now, this is a simplified implementation that doesn't use lineage tracking
   * to avoid circular dependencies with the execution module.
   *
   * @param task The task to execute
   * @tparam A Input type
   * @tparam B Output type
   * @return Effect that yields the partition result or throws exception
   */
  def executeWithRetry[A, B](task: com.ewoodbury.sparklet.runtime.api.RunnableTask[A, B]): F[Partition[B]] = {
    executeWithRetryInternal(task, attempt = 1)
  }

  private def executeWithRetryInternal[A, B](
    task: com.ewoodbury.sparklet.runtime.api.RunnableTask[A, B],
    attempt: Int
  ): F[Partition[B]] = {
    logger.debug(s"Executing task (attempt $attempt)")

    // Execute the task in a blocking context
    Async[F].blocking(task.run()).attempt.flatMap {
      case Right(partition) =>
        logger.debug(s"Task succeeded on attempt $attempt")
        Async[F].pure(partition)

      case Left(exception) =>
        logger.warn(s"Task failed on attempt $attempt", exception)

        // Check if we should retry based on the retry policy
        if (retryPolicy.shouldRetry(attempt, exception)) {
          val delay = retryPolicy.delayBeforeRetry(attempt)

          logger.info(
            s"Retrying task in ${delay.toMillis}ms " +
            s"(attempt ${attempt + 1}/${retryPolicy.maxRetries + 1})"
          )

          // Wait for the retry delay, then retry
          Async[F].sleep(delay) *> executeWithRetryInternal(task, attempt + 1)
        } else {
          // No more retries, try recovery if available
          recoveryManager match {
            case Some(manager) =>
              logger.info(s"Attempting recovery for failed task")
              manager.recoverFailedTask("unknown", exception).flatMap {
                case Some(_) =>
                  logger.warn(s"Recovery returned a result, but type is not compatible")
                  Async[F].raiseError(exception) // For now, recovery always fails
                case None =>
                  logger.error(s"Failed to recover task after $attempt attempts")
                  Async[F].raiseError(exception)
              }
            case None =>
              logger.error(s"Task failed permanently after $attempt attempts")
              Async[F].raiseError(exception)
          }
        }
    }
  }

  /**
   * Execute a simple RunnableTask without lineage support (for backward compatibility).
   *
   * @param task The task to execute
   * @tparam A Input type
   * @tparam B Output type
   * @return Effect that yields the partition result
   */
  def executeSimple[A, B](task: RunnableTask[A, B]): F[Partition[B]] = {
    Async[F].attempt(Async[F].blocking(task.run())).flatMap {
      case Right(partition) => Async[F].pure(partition)
      case Left(exception) =>
        logger.error(s"Simple task execution failed", exception)
        Async[F].raiseError(exception)
    }
  }
}

object TaskExecutionWrapper {

  /**
   * Create a TaskExecutionWrapper with retry policy only.
   */
  def withRetryPolicy[F[_]: Async](retryPolicy: RetryPolicy): TaskExecutionWrapper[F] =
    new TaskExecutionWrapper[F](retryPolicy, None)

  /**
   * Create a TaskExecutionWrapper with both retry policy and recovery manager.
   */
  def withRecovery[F[_]: Async](
    retryPolicy: RetryPolicy,
    recoveryManager: LineageRecoveryManager[F]
  ): TaskExecutionWrapper[F] =
    new TaskExecutionWrapper[F](retryPolicy, Some(recoveryManager))

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
