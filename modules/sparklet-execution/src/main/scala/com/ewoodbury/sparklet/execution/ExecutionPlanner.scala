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

  /**
   * Selects the write reason for a stage whose dependents include shuffle stages. SortBy wins
   * regardless of dependent order because range partitioning is the only scheme change; among the
   * rest, partitionBy (key-hash into its own count) beats repartition/coalesce; otherwise the
   * output is written with the default keyed write.
   */
  private[execution] def forDependents(
      shuffleDependents: Seq[StageBuilder.StageInfo],
  ): ShuffleWriteReason = {
    val wideOps = shuffleDependents.flatMap(_.wideOp)
    if (wideOps.exists(_.isInstanceOf[SortByWideOp[_, _]])) DownstreamSortBy
    else {
      // Filter by type first: collectFirst across case alternatives would pick whichever
      // dependent comes first in iteration order, not the highest-priority type
      val partitionBy = wideOps.collectFirst { case op: PartitionByWideOp =>
        DownstreamPartitionBy(op.meta.numPartitions)
      }
      val repartition = wideOps.collectFirst {
        case op: RepartitionWideOp => DownstreamRepartition(op.meta.numPartitions)
        case op: CoalesceWideOp => DownstreamRepartition(op.meta.numPartitions)
      }
      partitionBy.orElse(repartition).getOrElse(DownstreamShuffle)
    }
  }

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

    val shuffleDependents: Seq[StageBuilder.StageInfo] =
      dependentStages
        .map(depStageId => stageGraph.stages(depStageId))
        .filter(_.isShuffleStage)
        .toSeq

    if (shuffleDependents.isEmpty) Sync[F].pure(None)
    else {
      // A sortBy dependent is found first so the dispatch below is driven by the same value that
      // produced the reason: no reason/dispatch agreement to maintain by hand.
      val sortByDependent =
        shuffleDependents.find(_.wideOp.exists(_.isInstanceOf[SortByWideOp[_, _]]))

      val reason: ShuffleWriteReason =
        sortByDependent.fold(ShuffleWriteReason.forDependents(shuffleDependents))(_ =>
          ShuffleWriteReason.DownstreamSortBy,
        )

      val writeF: F[ShuffleId] = sortByDependent match {
        case Some(dep) =>
          shuffleHandler.handleSortByRangePartitionedOutput(stageInfo, results, stageGraph, dep.id)
        case None =>
          reason match {
            case ShuffleWriteReason.DownstreamPartitionBy(n) =>
              // Key-value data: write key-hashed into the partitionBy target count so the
              // partitionBy stage reads all of it and downstream bypasses stay correct
              shuffleHandler.handleKeyedOutput(stageInfo, results, n)
            case ShuffleWriteReason.DownstreamRepartition(n) =>
              shuffleHandler.handleRepartitionOrCoalesceOutput(stageInfo, results, n)
            case ShuffleWriteReason.DownstreamShuffle =>
              shuffleHandler.handleShuffleOutput(stageInfo, results)
            case ShuffleWriteReason.DownstreamSortBy =>
              Sync[F].raiseError(
                new IllegalStateException(
                  "sortBy write reason selected without a sortBy dependent; this is unreachable " +
                    "by construction of ShuffleWriteReason.forDependents",
                ),
              )
          }
      }

      logger.debug(s"Shuffle write for stage ${stageInfo.id.toInt}: $reason")
      writeF.map(id => Some((id, reason)))
    }
  }
