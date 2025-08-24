package com.ewoodbury.sparklet.runtime

import cats.effect.Async
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

import com.ewoodbury.sparklet.core.{Partition, LineageInfo}
import com.ewoodbury.sparklet.runtime.api.{ShuffleService, RunnableTask}

/**
 * Manages recovery of failed tasks through lineage-based recomputation.
 * Attempts to reconstruct and re-execute tasks using their dependency information.
 *
 * @param shuffleService The shuffle service to use for reading shuffle data
 * @param taskReconstructor The task reconstructor to use for reconstructing tasks
 * @param maxRecoveryAttempts The maximum number of recovery attempts to make
 * @tparam F The effect type
 */
class LineageRecoveryManager[F[_]: Async](
  shuffleService: ShuffleService,
  taskReconstructor: TaskReconstructor[F],
  maxRecoveryAttempts: Int = 3
) extends StrictLogging {

  // Cache for storing lineage information of running tasks
  private val lineageCache: ConcurrentHashMap[String, LineageInfo] = new ConcurrentHashMap()

  // Recovery statistics
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var stats = RecoveryStats(0, 0, 0)

  /**
   * Attempt to recover a failed task using its lineage information.
   *
   * @param taskId The ID of the failed task
   * @param originalException The original exception that caused the failure
   * @return Effect that yields Some(partition) if recovery succeeded, None otherwise
   */
  def recoverFailedTask(
    taskId: String,
    originalException: Throwable
  ): F[Option[Partition[Any]]] = {
    for {
      lineageOpt <- getTaskLineage(taskId)
      result <- lineageOpt match {
        case Some(lineage) if isRecoverable(lineage) =>
          logger.info(s"Attempting recovery for task $taskId with operation: ${lineage.operation}")
          attemptRecovery(lineage, originalException)
        case Some(lineage) =>
          logger.warn(s"Task $taskId is not recoverable (operation: ${lineage.operation})")
          Async[F].pure(None)
        case None =>
          logger.warn(s"No lineage information found for task $taskId")
          Async[F].pure(None)
      }
    } yield result
  }

  /**
   * Check if a task is recoverable based on its lineage information.
   */
  def isRecoverable(lineage: LineageInfo): Boolean = {
    isSupportedOperation(lineage.operation) && lineage.attemptCount < maxRecoveryAttempts
  }

  /**
   * Register lineage information for a task before execution.
   *
   * @param taskId The ID of the task
   * @param lineage The lineage information for the task
   * @return Effect that yields Unit
   */
  def registerTaskLineage(taskId: String, lineage: LineageInfo): F[Unit] = {
    Async[F].delay {
      lineageCache.put(taskId, lineage)
      logger.debug(s"Registered lineage for task $taskId: ${lineage.operation}")
    }
  }

  /**
   * Remove lineage information after successful task completion.
   *
   * @param taskId The ID of the task
   * @return Effect that yields Unit
   */
  def unregisterTaskLineage(taskId: String): F[Unit] = {
    Async[F].delay {
      lineageCache.remove(taskId)
    }
  }

  /**
   * Get lineage information for a task.
   *
   * @param taskId The ID of the task
   * @return Effect that yields Option[LineageInfo]
   */
  def getTaskLineage(taskId: String): F[Option[LineageInfo]] = {
    Async[F].delay(Option(lineageCache.get(taskId)))
  }

  /**
   * Update task lineage after a failed attempt.
   *
   * @param taskId The ID of the task
   * @param newAttempt The new attempt count
   * @return Effect that yields Unit
   */
  def updateTaskLineage(taskId: String, newAttempt: Int): F[Unit] = {
    Async[F].delay {
      Option(lineageCache.get(taskId)).foreach { lineage =>
        val updated = lineage.copy(attemptCount = newAttempt)
        lineageCache.put(taskId, updated)
      }
    }
  }

  /**
   * Attempt recovery using lineage information and task reconstruction.
   *
   * @param lineage The lineage information for the task
   * @param originalException The original exception that caused the failure
   * @return Effect that yields Some(partition) if recovery succeeded, None otherwise
   */
  private def attemptRecovery(
    lineage: LineageInfo,
    originalException: Throwable
  ): F[Option[Partition[Any]]] = {
    logger.debug(s"Attempting recovery for operation: ${lineage.operation}")

    for {
      reconstructedTaskOpt <- taskReconstructor.reconstructTask(lineage, originalException)
      result <- reconstructedTaskOpt match {
        case Some(reconstructedTask) =>
          executeReconstructedTask(reconstructedTask, lineage).attempt.flatMap {
            case Right(partition) =>
              updateRecoveryStats(success = true)
              logger.info(s"Successfully recovered task with operation: ${lineage.operation}")
              Async[F].pure(Some(partition))
            case Left(recoveryException) =>
              updateRecoveryStats(success = false)
              logger.warn(s"Recovery failed for operation: ${lineage.operation}. Original: ${originalException.getMessage}, Recovery: ${recoveryException.getMessage}")
              Async[F].pure(None)
          }
        case None =>
          updateRecoveryStats(success = false)
          logger.warn(s"Could not reconstruct task for operation: ${lineage.operation}")
          Async[F].pure(None)
      }
    } yield result
  }

  /**
   * Execute a reconstructed task safely.
   *
   * @param task Task to execute
   * @param lineage Lineage information for the task
   * @return Effect that yields Partition[Any]
   */
  private def executeReconstructedTask(
    task: RunnableTask[Any, Any],
    lineage: LineageInfo
  ): F[Partition[Any]] = {
    Async[F].delay {
      logger.debug(s"Executing reconstructed task for operation: ${lineage.operation}")
      task.run()
    }
  }

  /**
   * Check if an operation type supports recovery.
   */
  private def isSupportedOperation(operation: String): Boolean = {
    operation match {
      case op if op.startsWith("MapTask") => true
      case op if op.startsWith("FilterTask") => true
      case op if op.startsWith("FlatMapTask") => true
      case op if op.startsWith("DistinctTask") => true
      case op if op.startsWith("KeysTask") => true
      case op if op.startsWith("ValuesTask") => true
      case op if op.startsWith("MapValuesTask") => true
      case op if op.startsWith("FilterKeysTask") => true
      case op if op.startsWith("FilterValuesTask") => true
      case op if op.startsWith("FlatMapValuesTask") => true
      // Wide transformations - more complex, initially disabled
      case op if op.startsWith("JoinTask") => false
      case op if op.startsWith("ReduceByKeyTask") => false
      case _ => false
    }
  }

  /**
   * Update recovery statistics.
   */
  private def updateRecoveryStats(success: Boolean): F[Unit] = {
    Async[F].delay {
      val newStats = if (success) {
        stats.copy(
          totalAttempts = stats.totalAttempts + 1,
          successfulRecoveries = stats.successfulRecoveries + 1
        )
      } else {
        stats.copy(
          totalAttempts = stats.totalAttempts + 1,
          failedRecoveries = stats.failedRecoveries + 1
        )
      }
      stats = newStats
      logger.debug(s"Recovery stats updated - Success: $success, Total: ${stats.totalAttempts}")
    }
  }

  /**
   * Get recovery statistics (could be useful for monitoring).
   */
  def getRecoveryStats: F[RecoveryStats] = {
    Async[F].pure(stats)
  }
}

/**
 * Statistics about recovery attempts.
 */
final case class RecoveryStats(
  totalAttempts: Int,
  successfulRecoveries: Int,
  failedRecoveries: Int
)

object LineageRecoveryManager {

  /**
   * Create a LineageRecoveryManager with default settings.
   */
  def default[F[_]: Async](
    shuffleService: ShuffleService,
    taskReconstructor: TaskReconstructor[F]
  ): LineageRecoveryManager[F] =
    new LineageRecoveryManager[F](shuffleService, taskReconstructor)

  /**
   * Create a LineageRecoveryManager with custom settings.
   */
  def withMaxAttempts[F[_]: Async](
    shuffleService: ShuffleService,
    taskReconstructor: TaskReconstructor[F],
    maxRecoveryAttempts: Int
  ): LineageRecoveryManager[F] =
    new LineageRecoveryManager[F](shuffleService, taskReconstructor, maxRecoveryAttempts)
}
