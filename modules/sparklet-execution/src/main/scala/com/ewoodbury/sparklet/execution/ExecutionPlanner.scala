package com.ewoodbury.sparklet.execution

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.{Partition, ShuffleId, StageId}

/**
 * Why a stage's output was written to the shuffle service. Kept with the shuffle id so the write
 * policy is inspectable and testable rather than implicit in control flow.
 */
sealed trait ShuffleWriteReason

object ShuffleWriteReason:
  /** A downstream stage reads this output through a shuffle boundary. */
  case object DownstreamShuffle extends ShuffleWriteReason

  /** A downstream sortBy range-partitions this output to preserve global order. */
  case object DownstreamSortBy extends ShuffleWriteReason

  /** A downstream partitionBy reads this output; written key-hashed into its target count. */
  final case class DownstreamPartitionBy(numPartitions: Int) extends ShuffleWriteReason

  /** A downstream repartition/coalesce reads this output; written into its target count. */
  final case class DownstreamRepartition(numPartitions: Int) extends ShuffleWriteReason

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
  ): F[Map[StageId, Seq[Partition[_]]]] =
    executionOrder
      .foldLeftM((Map.empty[StageId, Seq[Partition[_]]], Map.empty[StageId, ShuffleId])) {
        case ((stageResults, shuffleMappings), stageId) =>
          val stageInfo = stageGraph.stages(stageId)
          for {
            _ <- Sync[F].delay(logger.info(s"ExecutionPlanner: executing stage ${stageId.toInt}"))
            inputPartitions <- Sync[F].delay(
              stageExecutor.getInputPartitionsForStage(stageInfo, stageResults, shuffleMappings),
            )
            results <- stageExecutor.executeStage(stageInfo, inputPartitions, shuffleMappings)
            updatedMappings <- writeShuffleIfNeeded(stageInfo, results, stageGraph).map {
              case Some((shuffleId, _reason)) => shuffleMappings + (stageInfo.id -> shuffleId)
              case None => shuffleMappings
            }
          } yield (stageResults + (stageId -> results), updatedMappings)
      }
      .map(_._1)

  /**
   * Persists this stage's output to the shuffle service if any dependent stage is a shuffle stage.
   * The write reason records which downstream requirement drove the write; when several dependents
   * need differently-partitioned data, the sort requirement wins because it is the only one that
   * changes the partitioning scheme (range instead of hash).
   */
  private def writeShuffleIfNeeded(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
      stageGraph: StageBuilder.StageGraph,
  ): F[Option[(ShuffleId, ShuffleWriteReason)]] = {
    val dependentStages: Iterable[StageId] =
      stageGraph.dependencies.filter(_._2.contains(stageInfo.id)).keys

    val shuffleDependents =
      dependentStages.map(depStageId => stageGraph.stages(depStageId)).filter(_.isShuffleStage)

    if (shuffleDependents.isEmpty) Sync[F].pure(None)
    else {
      val reason: ShuffleWriteReason = shuffleDependents
        .flatMap(_.wideOp)
        .collectFirst {
          case op: SortByWideOp[_, _] => ShuffleWriteReason.DownstreamSortBy
          case op: PartitionByWideOp =>
            ShuffleWriteReason.DownstreamPartitionBy(op.meta.numPartitions)
          case op: RepartitionWideOp =>
            ShuffleWriteReason.DownstreamRepartition(op.meta.numPartitions)
          case op: CoalesceWideOp =>
            ShuffleWriteReason.DownstreamRepartition(op.meta.numPartitions)
        }
        .getOrElse(ShuffleWriteReason.DownstreamShuffle)

      val sortByDependentId = shuffleDependents
        .find(_.wideOp.exists(_.isInstanceOf[SortByWideOp[_, _]]))
        .map(_.id)

      val writeF: F[ShuffleId] = reason match {
        case ShuffleWriteReason.DownstreamSortBy =>
          shuffleHandler.handleSortByRangePartitionedOutput(
            stageInfo,
            results,
            stageGraph,
            sortByDependentId.get,
          )
        case ShuffleWriteReason.DownstreamPartitionBy(n) =>
          // Key-value data: write key-hashed into the partitionBy target count so the
          // partitionBy stage reads all of it and downstream bypasses stay correct
          shuffleHandler.handleKeyedOutput(stageInfo, results, n)
        case ShuffleWriteReason.DownstreamRepartition(n) =>
          shuffleHandler.handleRepartitionOrCoalesceOutput(stageInfo, results, n)
        case ShuffleWriteReason.DownstreamShuffle =>
          shuffleHandler.handleShuffleOutput(stageInfo, results)
      }

      writeF.map(id => Some((id, reason)))
    }
  }
