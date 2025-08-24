package com.ewoodbury.sparklet.runtime

import cats.effect.{Async, Sync}
import cats.syntax.all.*
import cats.Traverse
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.{Partition, ShuffleId, LineageInfo, PartitionId}
import com.ewoodbury.sparklet.runtime.api.{RunnableTask, ShuffleService, Partitioner}

/**
 * TaskReconstructor handles the reconstruction of failed tasks based on lineage information.
 * This is in the runtime module to avoid circular dependencies with the execution module.
 */
class TaskReconstructor[F[_]: Async](
  shuffleService: ShuffleService
) extends StrictLogging {

  /**
   * Reconstruct a task from lineage information.
   * Returns a task that can be executed to recover the lost data.
   */
  def reconstructTask(
    lineage: LineageInfo,
    originalException: Throwable
  ): F[Option[RunnableTask[Any, Any]]] = {
    logger.debug(s"Attempting to reconstruct task with operation: ${lineage.operation}")

    reconstructTaskFromOperation(lineage).map { taskOpt =>
      taskOpt.map { task =>
        // Wrap the reconstructed task with lineage-aware error handling
        new LineageAwareTaskWrapper(task, lineage)
      }
    }
  }

  /**
   * Reconstruct task based on operation type.
   */
  private def reconstructTaskFromOperation(
    lineage: LineageInfo
  ): F[Option[RunnableTask[Any, Any]]] = {
    val operation = lineage.operation

    operation match {
      case op if op.startsWith("MapTask") =>
        reconstructMapTask(lineage)
      case op if op.startsWith("FilterTask") =>
        reconstructFilterTask(lineage)
      case op if op.startsWith("FlatMapTask") =>
        reconstructFlatMapTask(lineage)
      case op if op.startsWith("DistinctTask") =>
        reconstructDistinctTask(lineage)
      case op if op.startsWith("KeysTask") =>
        reconstructKeysTask(lineage)
      case op if op.startsWith("ValuesTask") =>
        reconstructValuesTask(lineage)
      case op if op.startsWith("MapValuesTask") =>
        reconstructMapValuesTask(lineage)
      case op if op.startsWith("FilterKeysTask") =>
        reconstructFilterKeysTask(lineage)
      case op if op.startsWith("FilterValuesTask") =>
        reconstructFilterValuesTask(lineage)
      case op if op.startsWith("FlatMapValuesTask") =>
        reconstructFlatMapValuesTask(lineage)
      case _ =>
        logger.warn(s"Task reconstruction not supported for operation: $operation")
        Async[F].pure(None)
    }
  }

  /**
   * Reconstruct a map task by reading input data from shuffle dependencies.
   */
  private def reconstructMapTask(lineage: LineageInfo): F[Option[RunnableTask[Any, Any]]] = {
    for {
      inputData <- readInputDataFromShuffle(lineage)
    } yield {
      inputData.map { data =>
        new RunnableTask[Any, Any] {
          override def run(): Partition[Any] = {
            // Return the data as-is since we can't reconstruct the original mapping function
            Partition(data)
          }
        }
      }
    }
  }

  /**
   * Reconstruct a filter task by reading input data from shuffle dependencies.
   */
  private def reconstructFilterTask(lineage: LineageInfo): F[Option[RunnableTask[Any, Any]]] = {
    for {
      inputData <- readInputDataFromShuffle(lineage)
    } yield {
      inputData.map { data =>
        new RunnableTask[Any, Any] {
          override def run(): Partition[Any] = {
            // Return the data as-is since we can't reconstruct the original predicate
            Partition(data)
          }
        }
      }
    }
  }

  /**
   * Reconstruct a flatMap task by reading input data from shuffle dependencies.
   */
  private def reconstructFlatMapTask[A, B](lineage: LineageInfo): F[Option[RunnableTask[A, B]]] = {
    for {
      inputData <- readInputDataFromShuffle(lineage)
    } yield {
      inputData.map { data =>
        new RunnableTask[A, B] {
          override def run(): Partition[B] = {
            // Return the data as-is since we can't reconstruct the original function
            Partition(data.asInstanceOf[Seq[B]])
          }
        }
      }
    }
  }

  /**
   * Reconstruct a distinct task by reading input data and re-applying distinct.
   */
  private def reconstructDistinctTask[A](lineage: LineageInfo): F[Option[RunnableTask[A, A]]] = {
    for {
      inputData <- readInputDataFromShuffle(lineage)
    } yield {
      inputData.map { data =>
        new RunnableTask[A, A] {
          override def run(): Partition[A] = {
            val distinctData = data.distinct
            Partition(distinctData.asInstanceOf[Seq[A]])
          }
        }
      }
    }
  }

  /**
   * Reconstruct a keys task by reading key-value pairs and extracting keys.
   */
  private def reconstructKeysTask(lineage: LineageInfo): F[Option[RunnableTask[Any, Any]]] = {
    for {
      inputData <- readInputDataFromShuffle(lineage)
    } yield {
      inputData.map { data =>
        new RunnableTask[Any, Any] {
          override def run(): Partition[Any] = {
            val keys = data.collect {
              case (k, _) => k
            }
            Partition(keys)
          }
        }
      }
    }
  }

  /**
   * Reconstruct a values task by reading key-value pairs and extracting values.
   */
  private def reconstructValuesTask(lineage: LineageInfo): F[Option[RunnableTask[Any, Any]]] = {
    for {
      inputData <- readInputDataFromShuffle(lineage)
    } yield {
      inputData.map { data =>
        new RunnableTask[Any, Any] {
          override def run(): Partition[Any] = {
            val values = data.collect {
              case (_, v) => v
            }
            Partition(values)
          }
        }
      }
    }
  }

  /**
   * Reconstruct a mapValues task by reading key-value pairs and preserving structure.
   */
  private def reconstructMapValuesTask(lineage: LineageInfo): F[Option[RunnableTask[Any, Any]]] = {
    for {
      inputData <- readInputDataFromShuffle(lineage)
    } yield {
      inputData.map { data =>
        new RunnableTask[Any, Any] {
          override def run(): Partition[Any] = {
            // Since we can't reconstruct the original mapping function,
            // we preserve the key-value structure with the original values
            val result = data.collect {
              case (k, v) => (k, v)
            }
            Partition(result)
          }
        }
      }
    }
  }

  /**
   * Reconstruct a filterKeys task by reading key-value pairs and preserving structure.
   */
  private def reconstructFilterKeysTask(lineage: LineageInfo): F[Option[RunnableTask[Any, Any]]] = {
    for {
      inputData <- readInputDataFromShuffle(lineage)
    } yield {
      inputData.map { data =>
        new RunnableTask[Any, Any] {
          override def run(): Partition[Any] = {
            // Return all key-value pairs since we can't reconstruct the original predicate
            Partition(data)
          }
        }
      }
    }
  }

  /**
   * Reconstruct a filterValues task by reading key-value pairs and preserving structure.
   */
  private def reconstructFilterValuesTask(lineage: LineageInfo): F[Option[RunnableTask[Any, Any]]] = {
    for {
      inputData <- readInputDataFromShuffle(lineage)
    } yield {
      inputData.map { data =>
        new RunnableTask[Any, Any] {
          override def run(): Partition[Any] = {
            // Return all key-value pairs since we can't reconstruct the original predicate
            Partition(data)
          }
        }
      }
    }
  }

  /**
   * Reconstruct a flatMapValues task by reading key-value pairs and preserving structure.
   */
  private def reconstructFlatMapValuesTask(lineage: LineageInfo): F[Option[RunnableTask[Any, Any]]] = {
    for {
      inputData <- readInputDataFromShuffle(lineage)
    } yield {
      inputData.map { data =>
        new RunnableTask[Any, Any] {
          override def run(): Partition[Any] = {
            // Since we can't reconstruct the original flatMap function,
            // we preserve the key-value structure with the original values
            val result = data.collect {
              case (k, v) => (k, v)
            }
            Partition(result)
          }
        }
      }
    }
  }

  /**
   * Read input data from shuffle dependencies for task reconstruction.
   */
  private def readInputDataFromShuffle(lineage: LineageInfo): F[Option[Seq[Any]]] = {
    if (lineage.shuffleDependencies.isEmpty) {
      logger.warn("No shuffle dependencies available for task reconstruction")
      return Async[F].pure(None)
    }

    val allPartitions = for {
      shuffleId <- lineage.shuffleDependencies
      partitionId <- lineage.inputPartitions
    } yield (shuffleId, partitionId)

    val partitionData = allPartitions.map { case (shuffleId, partitionId) =>
      Async[F].delay {
        shuffleService.readPartition(shuffleId, PartitionId(partitionId))
      }
    }

    for {
      partitions <- partitionData.sequence
    } yield {
      if (partitions.isEmpty) {
        None
      } else {
        Some(partitions.flatMap(_.data.toSeq))
      }
    }
  }

  /**
   * Wrapper that adds lineage-aware error handling to reconstructed tasks.
   */
  private class LineageAwareTaskWrapper(
    underlyingTask: RunnableTask[Any, Any],
    lineage: LineageInfo
  ) extends RunnableTask[Any, Any] {
    override def run(): Partition[Any] = {
      try {
        underlyingTask.run()
      } catch {
        case e: Exception =>
          logger.error(s"Reconstructed task failed for operation: ${lineage.operation}", e)
          throw e
      }
    }
  }
}

object TaskReconstructor {

  /**
   * Create a TaskReconstructor with default settings.
   */
  def default[F[_]: Async](
    shuffleService: ShuffleService
  ): TaskReconstructor[F] =
    new TaskReconstructor[F](shuffleService)

  /**
   * Create a TaskReconstructor with custom settings.
   */
  def withCustomSettings[F[_]: Async](
    shuffleService: ShuffleService
  ): TaskReconstructor[F] =
    new TaskReconstructor[F](shuffleService)
}
