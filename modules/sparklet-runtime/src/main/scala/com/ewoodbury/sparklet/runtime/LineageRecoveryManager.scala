package com.ewoodbury.sparklet.runtime

import cats.effect.{Async, Ref}
import cats.syntax.all.*
import cats.data.OptionT
import com.typesafe.scalalogging.StrictLogging
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import com.ewoodbury.sparklet.core.{Partition, LineageInfo}
import com.ewoodbury.sparklet.runtime.api.RunnableTask

/**
 * Manages recovery of failed tasks through lineage-based recomputation.
 * Features exponential backoff and enhanced statistics tracking.
 * 
 * TODO: Add shuffle service to the constructor to allow for reading shuffle data
 * when we're ready to support recovery of complex operations (joins, aggregations, etc.)
 *
 * @param taskReconstructor The task reconstructor to use for reconstructing tasks
 * @param maxRecoveryAttempts The maximum number of recovery attempts to make
 * @param baseBackoffDelay Base delay for exponential backoff between attempts
 * @tparam F The effect type
 */
class LineageRecoveryManager[F[_]: Async](
  taskReconstructor: TaskReconstructor[F],
  maxRecoveryAttempts: Int = 3,
  baseBackoffDelay: FiniteDuration = 200.millis
) extends StrictLogging {

  // Cache for storing lineage information of running tasks
  private val lineageCache: ConcurrentHashMap[String, TaskLineageEntry] = new ConcurrentHashMap()

  // Thread-safe recovery statistics using Ref
  private val statsRef: F[Ref[F, RecoveryStats]] = Async[F].ref(RecoveryStats.empty)

  /**
   * Attempt to recover a failed task using its lineage information.
   * Includes exponential backoff between attempts.
   *
   * @param taskId The ID of the failed task
   * @param originalException The original exception that caused the failure
   * @return Effect that yields Some(partition) if recovery succeeded, None otherwise
   */
  def recoverFailedTask(
    taskId: String,
    originalException: Throwable
  ): F[Option[Partition[Any]]] = {
    (for {
      entry <- OptionT(getTaskLineageEntry(taskId))
      _ = logger.info(s"Attempting recovery for task $taskId (attempt ${entry.attemptCount + 1}/$maxRecoveryAttempts)")
      
      // Check if recovery should proceed
      canRecover = isRecoverable(entry.lineage)
      _ <- OptionT.when(canRecover)(Async[F].unit)
      
      // Apply exponential backoff delay if this isn't the first attempt
      backoffDelay = calculateBackoffDelay(entry.attemptCount)
      _ <- OptionT.liftF(if (backoffDelay > Duration.Zero) {
        logger.debug(s"Applying backoff delay of ${backoffDelay.toMillis}ms for task $taskId")
        Async[F].sleep(backoffDelay)
      } else Async[F].unit)
      
      // Update attempt count
      _ <- OptionT.liftF(updateTaskLineageAttempt(taskId, entry.attemptCount + 1))
      
      // Attempt actual recovery
      result <- OptionT(attemptRecovery(entry.lineage, originalException))
      
    } yield result).value.handleErrorWith { error =>
      logger.error(s"Recovery attempt failed for task $taskId", error)
      updateStats(_.incrementFailures) *> Async[F].pure(None)
    }
  }

  /**
   * Check if a task is recoverable based on its lineage information.
   */
  def isRecoverable(lineage: LineageInfo): Boolean = {
    isRecoverableOperation(lineage.operation) && lineage.attemptCount < maxRecoveryAttempts
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
      val entry = TaskLineageEntry(lineage, 0, Instant.now())
      lineageCache.put(taskId, entry)
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
    Async[F].delay(Option(lineageCache.get(taskId)).map(_.lineage))
  }

  /**
   * Update task lineage after a failed attempt.
   *
   * @param taskId The ID of the task
   * @param newAttempt The new attempt count
   * @return Effect that yields Unit
   */
  def updateTaskLineage(taskId: String, newAttempt: Int): F[Unit] = {
    updateTaskLineageAttempt(taskId, newAttempt)
  }

  /**
   * Get current lineage entry for a task.
   */
  private def getTaskLineageEntry(taskId: String): F[Option[TaskLineageEntry]] = {
    Async[F].delay(Option(lineageCache.get(taskId)))
  }

  /**
   * Update task lineage attempt count.
   */
  private def updateTaskLineageAttempt(taskId: String, newAttempt: Int): F[Unit] = {
    Async[F].delay {
      Option(lineageCache.get(taskId)).foreach { entry =>
        val updated = entry.copy(attemptCount = newAttempt)
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
    val startTime = Instant.now()

    for {
      _ <- updateStats(_.incrementAttempts)
      reconstructedTaskOpt <- taskReconstructor.reconstructTask(lineage, originalException)
      result <- reconstructedTaskOpt match {
        case Some(reconstructedTask) =>
          executeReconstructedTask(reconstructedTask, lineage).attempt.flatMap {
            case Right(partition) =>
              val duration = java.time.Duration.between(startTime, Instant.now())
              updateStats(_.recordSuccess(duration)) *>
              logger.info(s"Successfully recovered task with operation: ${lineage.operation} in ${duration.toMillis}ms").pure[F] *>
              Some(partition).pure[F]
            case Left(recoveryException) =>
              val duration = java.time.Duration.between(startTime, Instant.now())
              updateStats(_.recordFailure(duration)) *>
              logger.warn(s"Recovery failed for operation: ${lineage.operation} after ${duration.toMillis}ms. Original: ${originalException.getMessage}, Recovery: ${recoveryException.getMessage}").pure[F] *>
              None.pure[F]
          }
        case None =>
          updateStats(_.incrementReconstructionFailures) *>
          logger.warn(s"Could not reconstruct task for operation: ${lineage.operation}").pure[F] *>
          None.pure[F]
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
   * Calculate exponential backoff delay based on attempt number.
   * Formula: baseDelay * 2^(attempt-1), capped at 30 seconds
   */
  private def calculateBackoffDelay(attemptNumber: Int): FiniteDuration = {
    if (attemptNumber == 0) Duration.Zero
    else {
      val exponentialDelay = baseBackoffDelay.toMillis * math.pow(2, attemptNumber - 1)
      val cappedDelay = math.min(exponentialDelay, 30000) // Cap at 30 seconds
      cappedDelay.millis
    }
  }

  /**
   * Check if an operation type supports recovery.
   */
  private def isRecoverableOperation(operation: String): Boolean = {
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
   * Thread-safe stats update using Ref.
   */
  private def updateStats(f: RecoveryStats => RecoveryStats): F[Unit] = {
    statsRef.flatMap(_.update(f))
  }

  /**
   * Get recovery statistics.
   */
  def getRecoveryStats: F[RecoveryStats] = {
    statsRef.flatMap(_.get)
  }
}

/**
 * Internal representation of task lineage with metadata.
 */
private final case class TaskLineageEntry(
  lineage: LineageInfo,
  attemptCount: Int,
  registrationTime: Instant
)

/**
 * Enhanced statistics with timing information and better metrics.
 */
final case class RecoveryStats(
  totalAttempts: Int,
  successfulRecoveries: Int,
  failedRecoveries: Int,
  reconstructionFailures: Int,
  totalRecoveryTimeMs: Long,
  lastUpdated: Instant
) {
  def incrementAttempts: RecoveryStats = copy(
    totalAttempts = totalAttempts + 1,
    lastUpdated = Instant.now()
  )
  
  def incrementFailures: RecoveryStats = copy(
    failedRecoveries = failedRecoveries + 1,
    lastUpdated = Instant.now()
  )
  
  def incrementReconstructionFailures: RecoveryStats = copy(
    reconstructionFailures = reconstructionFailures + 1,
    lastUpdated = Instant.now()
  )
  
  def recordSuccess(duration: java.time.Duration): RecoveryStats = copy(
    successfulRecoveries = successfulRecoveries + 1,
    totalRecoveryTimeMs = totalRecoveryTimeMs + duration.toMillis,
    lastUpdated = Instant.now()
  )
  
  def recordFailure(duration: java.time.Duration): RecoveryStats = copy(
    failedRecoveries = failedRecoveries + 1,
    totalRecoveryTimeMs = totalRecoveryTimeMs + duration.toMillis,
    lastUpdated = Instant.now()
  )
  
  def successRate: Double = {
    if (totalAttempts == 0) 0.0
    else successfulRecoveries.toDouble / totalAttempts.toDouble
  }
  
  def averageRecoveryTimeMs: Option[Double] = {
    if (successfulRecoveries == 0) None
    else Some(totalRecoveryTimeMs.toDouble / successfulRecoveries.toDouble)
  }
}

object RecoveryStats {
  def empty: RecoveryStats = RecoveryStats(
    totalAttempts = 0,
    successfulRecoveries = 0,
    failedRecoveries = 0,
    reconstructionFailures = 0,
    totalRecoveryTimeMs = 0L,
    lastUpdated = Instant.now()
  )
}

object LineageRecoveryManager {

  /**
   * Create a LineageRecoveryManager with default settings.
   */
  def default[F[_]: Async](
    taskReconstructor: TaskReconstructor[F]
  ): LineageRecoveryManager[F] =
    new LineageRecoveryManager[F](taskReconstructor)

  /**
   * Create a LineageRecoveryManager with custom settings.
   */
  def withMaxAttempts[F[_]: Async](
    taskReconstructor: TaskReconstructor[F],
    maxRecoveryAttempts: Int
  ): LineageRecoveryManager[F] =
    new LineageRecoveryManager[F](taskReconstructor, maxRecoveryAttempts)

  /**
   * Create a LineageRecoveryManager with custom backoff settings.
   */
  def withBackoff[F[_]: Async](
    taskReconstructor: TaskReconstructor[F],
    maxRecoveryAttempts: Int,
    baseBackoffDelay: FiniteDuration
  ): LineageRecoveryManager[F] =
    new LineageRecoveryManager[F](taskReconstructor, maxRecoveryAttempts, baseBackoffDelay)
}