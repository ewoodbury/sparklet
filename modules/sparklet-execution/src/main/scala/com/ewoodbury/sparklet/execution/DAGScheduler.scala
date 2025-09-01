package com.ewoodbury.sparklet.execution
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.*
import com.ewoodbury.sparklet.runtime.LineageRecoveryManager
import com.ewoodbury.sparklet.runtime.api.{Partitioner, ShuffleService, TaskScheduler}

final class DAGScheduler[F[_]: Sync](
    shuffle: ShuffleService,
    scheduler: TaskScheduler[F],
    partitioner: Partitioner,
    recoveryManager: Option[LineageRecoveryManager[F]] = None,
) extends StrictLogging {

  // Create instances of the new components
  private val joinExecutor = new JoinExecutor[F](shuffle, scheduler)
  private val shuffleHandler = new ShuffleHandler[F](shuffle, partitioner)
  private val stageExecutor = new StageExecutor[F](shuffle, scheduler, joinExecutor)
  private val executionPlanner = new ExecutionPlanner[F](stageExecutor, shuffleHandler)

  /**
   * Executes a plan using multi-stage execution, handling shuffle boundaries.
   */
  def execute[A](plan: Plan[A]): F[Iterable[A]] = executeWithRecovery(plan)

  /**
   * Executes a plan with recovery support using multi-stage execution.
   */
  def executeWithRecovery[A](plan: Plan[A]): F[Iterable[A]] =
    for {
      _ <- Sync[F].delay(
        logger.info(
          "DAGScheduler: starting multi-stage execution" +
            (if (recoveryManager.isDefined) " with recovery support" else ""),
        ),
      )
      stageGraph <- Sync[F].delay(StageBuilder.buildStageGraph(plan))
      _ <- Sync[F].delay(
        logger.debug(s"DAGScheduler: built stage graph with ${stageGraph.stages.size} stages"),
      )
      executionOrder <- Sync[F].delay(TopologicalSort.sort(stageGraph.dependencies))
      _ <- Sync[F].delay(
        logger.debug(
          s"DAGScheduler: execution order: ${executionOrder.map(_.toInt).mkString(" -> ")}",
        ),
      )
      stageResults <- executionPlanner.runStagesWithRecovery(
        stageGraph,
        executionOrder,
        recoveryManager,
      )
      finalData <- Sync[F].delay {
        val finalResults = stageResults(stageGraph.finalStageId)
        // Type-safe extraction of final results data
        finalResults.flatMap(partition => 
          partition.data match {
            case iter: Iterable[A @unchecked] => iter
            case other => throw new ClassCastException(s"Expected Iterable[A], got ${other.getClass}")
          }
        )
      }
      _ <- Sync[F].delay(logger.info("DAGScheduler: multi-stage execution completed"))
    } yield finalData
}

object DAGScheduler:
  def requiresDAGScheduling[A](plan: Plan[A]): Boolean = {
    PlanWide.isWide(plan)
  }
