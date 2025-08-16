package com.ewoodbury.sparklet.execution

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.{Partition, Plan, ShuffleId, StageId}
import scala.collection.mutable

/**
 * Planner for coordinating stage execution.
 */
final class ExecutionPlanner[F[_]: Sync](
    stageExecutor: StageExecutor[F],
    shuffleHandler: ShuffleHandler[F],
) extends StrictLogging:

  /**
   * Iterates through the stages in topological order, executing each and materializing shuffle
   * outputs when needed. Returns a map of stage results.
   */
  def runStages(
      stageGraph: StageBuilder.StageGraph,
      executionOrder: List[StageId],
  ): F[mutable.Map[StageId, Seq[Partition[_]]]] = {
    val stageResults = mutable.Map[StageId, Seq[Partition[_]]]()
    val stageToShuffleId = mutable.Map[StageId, ShuffleId]()

    executionOrder
      .foldLeftM(()) { (_, stageId) =>
        val stageInfo = stageGraph.stages(stageId)
        for {
          _ <- Sync[F].delay(logger.info(s"ExecutionPlanner: executing stage ${stageId.toInt}"))
          inputPartitions <- Sync[F].delay(
            stageExecutor.getInputPartitionsForStage(stageInfo, stageResults, stageToShuffleId),
          )
          results <- stageExecutor.executeStage(stageInfo, inputPartitions, stageToShuffleId)
          _ <- Sync[F].delay { stageResults(stageId) = results }
          _ <- writeShuffleIfNeeded(stageInfo, results, stageGraph, stageToShuffleId)
        } yield ()
      }
      .as(stageResults)
  }

  /**
   * If any dependent stage is a shuffle stage, persist this stage's output to the shuffle service.
   * For sortBy dependencies we use a special keying strategy to preserve element order.
   */
  private def writeShuffleIfNeeded(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
      stageGraph: StageBuilder.StageGraph,
      stageToShuffleId: mutable.Map[StageId, ShuffleId],
  ): F[Unit] = {
    val dependentStages: Iterable[StageId] =
      stageGraph.dependencies.filter(_._2.contains(stageInfo.id)).keys

    val needsShuffleOutput =
      dependentStages.exists(depStageId => stageGraph.stages(depStageId).isShuffleStage)

    if (!needsShuffleOutput) Sync[F].unit
    else {
      val hasSortByDependent = dependentStages.exists(depStageId =>
        stageGraph
          .stages(depStageId)
          .shuffleOperation
          .exists(_.isInstanceOf[Plan.SortByOp[_, _]]),
      )

      val repartitionDep = dependentStages
        .flatMap(id => stageGraph.stages(id).shuffleOperation.toSeq)
        .collectFirst { case op: Plan.RepartitionOp[_] => op }
      val coalesceDep = dependentStages
        .flatMap(id => stageGraph.stages(id).shuffleOperation.toSeq)
        .collectFirst { case op: Plan.CoalesceOp[_] => op }
      val partitionByDep = dependentStages
        .flatMap(id => stageGraph.stages(id).shuffleOperation.toSeq)
        .collectFirst { case op: Plan.PartitionByOp[_, _] => op }

      val writeF: F[ShuffleId] =
        if (hasSortByDependent) {
          // Find the dependent sortBy stage and use its configuration to range-partition output
          val sortByDependentId = dependentStages
            .find(depStageId =>
              stageGraph
                .stages(depStageId)
                .shuffleOperation
                .exists(_.isInstanceOf[Plan.SortByOp[_, _]]),
            )
            .get

          shuffleHandler.handleSortByRangePartitionedOutput(stageInfo, results, stageGraph, sortByDependentId)
        } else
          repartitionDep
            .map(op => shuffleHandler.handleRepartitionOrCoalesceOutput(stageInfo, results, op.numPartitions))
            .orElse(
              coalesceDep
                .map(op => shuffleHandler.handleRepartitionOrCoalesceOutput(stageInfo, results, op.numPartitions)),
            )
            .orElse(
              partitionByDep
                .map(op => shuffleHandler.handleRepartitionOrCoalesceOutput(stageInfo, results, op.numPartitions)),
            )
            .getOrElse(shuffleHandler.handleShuffleOutput(stageInfo, results))

      writeF.flatMap { shuffleId =>
        Sync[F].delay { stageToShuffleId(stageInfo.id) = shuffleId }
      }
    }
  }