package com.ewoodbury.sparklet.execution
import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.*
import com.ewoodbury.sparklet.runtime.api.{Partitioner, ShuffleService, TaskScheduler}

/**
 * The single execution entry point: compiles a plan into a stage graph and runs its stages in
 * topological order, coordinating shuffles between them.
 */
final class DAGScheduler[F[_]: Sync](
    shuffle: ShuffleService,
    scheduler: TaskScheduler[F],
    partitioner: Partitioner,
) extends StrictLogging {

  private val joinExecutor = new JoinExecutor[F](shuffle, scheduler)
  private val shuffleHandler = new ShuffleHandler[F](shuffle, partitioner)
  private val stageExecutor = new StageExecutor[F](shuffle, scheduler, joinExecutor)
  private val executionPlanner = new ExecutionPlanner[F](stageExecutor, shuffleHandler)

  /**
   * Executes a plan using multi-stage execution, handling shuffle boundaries.
   */
  def execute[A](plan: Plan[A]): F[Iterable[A]] =
    executePartitions(plan).map(_.flatMap(_.data))

  /**
   * Executes a plan and returns the final stage's output with partition boundaries preserved.
   * Callers should flatten only at the outermost boundary so partition-aware consumers (such as
   * aggregate) can operate per partition.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def executePartitions[A](plan: Plan[A]): F[Seq[Partition[A]]] =
    runStages(plan).map { case (stageGraph, stageResults) =>
      stageResults(stageGraph.finalStageId).asInstanceOf[Seq[Partition[A]]]
    }

  /**
   * Runs all stages in topological order and returns the results of every stage, keyed by stage
   * ID.
   */
  private def runStages[A](
      plan: Plan[A],
  ): F[(StageBuilder.StageGraph, Map[StageId, Seq[Partition[_]]])] =
    for {
      _ <- Sync[F].delay(logger.info("DAGScheduler: starting multi-stage execution"))
      stageGraph <- Sync[F].delay(StageBuilder.buildStageGraph(plan))
      _ <- Sync[F].delay(
        logger.debug(s"DAGScheduler: built stage graph with ${stageGraph.stages.size} stages"),
      )
      executionOrder <- Sync[F].delay(
        TopologicalSort.sort(stageGraph.stages.keySet, stageGraph.dependencies),
      )
      _ <- Sync[F].delay(
        logger.debug(
          s"DAGScheduler: execution order: ${executionOrder.map(_.toInt).mkString(" -> ")}",
        ),
      )
      stageResults <- executionPlanner.runStages(stageGraph, executionOrder)
      _ <- Sync[F].delay(logger.info("DAGScheduler: multi-stage execution completed"))
    } yield (stageGraph, stageResults)
}
