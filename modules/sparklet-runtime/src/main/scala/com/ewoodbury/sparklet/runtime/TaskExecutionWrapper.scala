package com.ewoodbury.sparklet.runtime

import cats.effect.Async
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.{LineageInfo, Partition, RetryPolicy}
import com.ewoodbury.sparklet.runtime.api.RunnableTask

/**
 * Wrapper that executes tasks with retry logic and lineage tracking. Handles transient failures
 * and provides detailed execution information.
 */
class TaskExecutionWrapper[F[_]: Async] private (
    retryPolicy: RetryPolicy,
    recoveryManager: Option[LineageRecoveryManager[F]],
) extends StrictLogging {

  /**
   * Execute a task with retry logic and lineage tracking.
   *
   * @param task
   *   The task to execute
   * @param taskId
   *   Unique identifier for the task (used for lineage tracking)
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

  /**
   * Execute a task with lineage tracking and recovery.
   *
   * @param task
   *   The task to execute
   * @param lineage
   *   The lineage information for the task
   * @tparam A
   *   Input type
   * @tparam B
   *   Output type
   * @return
   *   Effect that yields the partition result or throws exception
   */
  def executeWithLineage[A, B](task: RunnableTask[A, B], lineage: LineageInfo): F[Partition[B]] = {
    val taskId = s"${lineage.stageId}-${lineage.taskId}"

    // Register lineage before execution
    recoveryManager match {
      case Some(manager) =>
        manager.registerTaskLineage(taskId, lineage) *> executeWithRetryInternal(
          task,
          taskId,
          attempt = 1,
        )
      case None =>
        executeWithRetryInternal(task, taskId, attempt = 1)
    }
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
        // Clean up lineage on success
        recoveryManager.foreach(_.unregisterTaskLineage(taskId))
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

          // Update lineage with new attempt count
          recoveryManager.foreach(_.updateTaskLineage(taskId, attempt + 1))

          // Wait for the retry delay, then retry
          Async[F].sleep(delay) *> executeWithRetryInternal(task, taskId, attempt + 1)
        } else {
          // No more retries, try recovery if available
          recoveryManager match {
            case Some(manager) =>
              logger.info(s"Attempting recovery for failed task $taskId")
              manager.recoverFailedTask(taskId, exception).flatMap {
                case Some(recoveredPartition) =>
                  logger.info(s"Successfully recovered task $taskId")
                  // Clean up lineage after successful recovery
                  manager.unregisterTaskLineage(taskId) *>
                    // Cast the recovered partition to the expected type
                    // This is safe because recovery preserves the original data type
                    Async[F].pure(recoveredPartition.asInstanceOf[Partition[B]])
                case None =>
                  logger.error(s"Failed to recover task $taskId after $attempt attempts")
                  // Clean up lineage after failed recovery
                  manager.unregisterTaskLineage(taskId) *>
                    Async[F].raiseError(exception)
              }
            case None =>
              logger.error(s"Task $taskId failed permanently after $attempt attempts")
              Async[F].raiseError(exception)
          }
        }
    }
  }

  /**
   * Execute a simple RunnableTask without lineage support (for backward compatibility).
   *
   * @param task
   *   The task to execute
   * @tparam A
   *   Input type
   * @tparam B
   *   Output type
   * @return
   *   Effect that yields the partition result
   */
  def executeSimple[A, B](task: RunnableTask[A, B]): F[Partition[B]] = {
    Async[F].attempt(Async[F].blocking(task.run())).flatMap {
      case Right(partition) => Async[F].pure(partition)
      case Left(exception) =>
        logger.error("Simple task execution failed", exception)
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
      recoveryManager: LineageRecoveryManager[F],
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
