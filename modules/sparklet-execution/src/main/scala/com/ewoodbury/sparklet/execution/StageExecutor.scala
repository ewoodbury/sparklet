package com.ewoodbury.sparklet.execution

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.{Partition, PartitionId, Plan, ShuffleId, StageId}
import com.ewoodbury.sparklet.runtime.api.{ShuffleService, TaskScheduler}

/**
 * Executor for handling stage execution.
 *
 * Named erasure boundary: stages transport data as `Partition[_]`, and each wide-op handler casts
 * the transport partitions back to the record types its `WideOp` describes. Safe because the stage
 * graph is built from the same typed operations that the handlers dispatch on.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Equals",
  ),
)
final class StageExecutor[F[_]: Sync](
    shuffle: ShuffleService,
    scheduler: TaskScheduler[F],
    joinExecutor: JoinExecutor[F],
) extends StrictLogging:

  /**
   * Gets input partitions for a stage, returning them with a wildcard type `_`. This avoids
   * casting at the source, deferring it to the execution function.
   */
  def getInputPartitionsForStage(
      stageInfo: StageBuilder.StageInfo,
      stageResults: Map[StageId, Seq[Partition[_]]],
      stageToShuffleId: Map[StageId, ShuffleId],
  ): Seq[Partition[_]] = {
    stageInfo.inputSources.flatMap {
      case StageBuilder.SourceInput(partitions) =>
        partitions // No cast needed

      case StageBuilder.StageOutput(depStageId) =>
        // Read the already computed output of the dependent stage
        stageResults.getOrElse(depStageId, Seq.empty[Partition[_]])

      case StageBuilder.ShuffleInput(upstreamStageId, _side, numPartitions) =>
        // Handle both single-input and multi-input shuffle operations uniformly
        val actualShuffleId = stageToShuffleId.getOrElse(
          upstreamStageId,
          throw new IllegalStateException(
            s"Missing shuffle id for upstream stage ${upstreamStageId.toInt} feeding stage ${stageInfo.id.toInt}",
          ),
        )
        (0 until numPartitions).map { partitionIdx =>
          shuffle.readPartition[Any, Any](actualShuffleId, PartitionId(partitionIdx))
        }
    }
  }

  /**
   * Executes a single stage using type-safe dispatch based on stage operations. Uses typed helpers
   * to eliminate unsafe casting and improve type safety.
   */
  def executeStage(
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[_]],
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    if (inputPartitions.isEmpty) then
      Sync[F].delay(logger.warn(s"Stage ${stageInfo.id.toInt} has no input partitions")) *> Sync[F]
        .pure(Seq.empty)
    else {
      stageInfo match {
        case info if info.isShuffleStage =>
          executeShuffleStageTyped(info, inputPartitions, stageToShuffleId)
        case info =>
          executeNarrowStageTyped(info, inputPartitions, stageToShuffleId)
      }
    }
  }

  /**
   * Type-safe execution of narrow stages using the Operation ADT.
   *
   * Wraps around the generic executeNarrowStage method, casting inputs to `Any`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def executeNarrowStageTyped(
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[_]],
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    val anyPartitions = inputPartitions.asInstanceOf[Seq[Partition[Any]]]
    executeNarrowStage(stageInfo, anyPartitions, stageToShuffleId)
  }

  /**
   * Type-safe execution of shuffle stages using operation-specific handlers.
   */
  private def executeShuffleStageTyped(
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[_]],
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    stageInfo.wideOp match {
      case Some(op: JoinWideOp) =>
        executeJoinOperation(stageInfo, op, stageToShuffleId)
      case Some(op: SortByWideOp[_, _]) =>
        executeSortByOperation(op, inputPartitions)
      case Some(_: GroupByKeyWideOp) =>
        executeGroupByKeyOperation(inputPartitions)
      case Some(op: ReduceByKeyWideOp[_]) =>
        executeReduceByKeyOperation(op, inputPartitions)
      case Some(_: CoGroupWideOp) =>
        executeCoGroupOperation(stageInfo, stageToShuffleId)
      case Some(_: RepartitionWideOp) | Some(_: CoalesceWideOp) =>
        executeRepartitionOperation(inputPartitions)
      case Some(_: PartitionByWideOp) =>
        // Upstream wrote key-hashed (K, V) tuples; the partitioning already happened in the
        // shuffle write, so the stage itself is an identity pass-through
        Sync[F].pure(inputPartitions)
      case other =>
        Sync[F].raiseError(
          new IllegalStateException(
            s"Stage ${stageInfo.id.toInt} has no executable wide operation (found: $other)",
          ),
        )
    }
  }

  /**
   * Submits one `StageTask` per input partition and returns results in input order. Shared by
   * narrow stages and by shuffle stages whose wide op is a per-partition local operator.
   */
  private def executePerPartition[A, B](
      partitions: Seq[Partition[A]],
      stage: Stage[A, B],
  ): F[Seq[Partition[_]]] = {
    val tasks = partitions.map(partition => Task.StageTask(partition, stage))
    scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
  }

  /**
   * Resolves the left/right shuffle ids and partition count for a two-input shuffle stage.
   */
  private def resolveBinaryShuffleInputs(
      stageInfo: StageBuilder.StageInfo,
      stageToShuffleId: Map[StageId, ShuffleId],
      operationName: String,
  ): (ShuffleId, ShuffleId, Int) = {
    val shuffleInputs = stageInfo.inputSources.collect { case s: StageBuilder.ShuffleInput =>
      s
    }
    def requireSide(side: StageBuilder.Side): StageBuilder.ShuffleInput =
      shuffleInputs
        .find(_.side.contains(side))
        .getOrElse(
          throw new IllegalStateException(
            s"$operationName missing $side input for stage ${stageInfo.id.toInt}",
          ),
        )
    def requireShuffleId(input: StageBuilder.ShuffleInput): ShuffleId =
      stageToShuffleId.getOrElse(
        input.stageId,
        throw new IllegalStateException(
          s"Missing shuffle id for $operationName upstream stage ${input.stageId.toInt}",
        ),
      )
    val leftInput = requireSide(StageBuilder.Side.Left)
    val rightInput = requireSide(StageBuilder.Side.Right)
    (requireShuffleId(leftInput), requireShuffleId(rightInput), leftInput.numPartitions)
  }

  /**
   * Type-safe handler for join operations.
   */
  private def executeJoinOperation(
      stageInfo: StageBuilder.StageInfo,
      joinOp: JoinWideOp,
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    val (leftShuffleId, rightShuffleId, numPartitions) =
      resolveBinaryShuffleInputs(stageInfo, stageToShuffleId, "Join")

    val strategy =
      joinOp.meta.joinStrategy.getOrElse(
        joinExecutor.selectJoinStrategy(leftShuffleId, rightShuffleId),
      )

    logger.info(s"Using join strategy: $strategy for stage ${stageInfo.id.toInt}")

    strategy match {
      case Plan.JoinStrategy.Broadcast =>
        joinExecutor.executeBroadcastHashJoin(leftShuffleId, rightShuffleId, numPartitions)
      case Plan.JoinStrategy.SortMerge =>
        joinExecutor.executeSortMergeJoin(leftShuffleId, rightShuffleId, numPartitions)
      case Plan.JoinStrategy.ShuffleHash =>
        joinExecutor.executeShuffleHashJoin(leftShuffleId, rightShuffleId, numPartitions)
    }
  }

  /**
   * Local sort of each range-partitioned `(sortKey, element)` partition. Concatenating the results
   * in partition order is globally ordered; there is no driver-side merge.
   */
  private def executeSortByOperation[A, S](
      sortBy: SortByWideOp[A, S],
      inputPartitions: Seq[Partition[_]],
  ): F[Seq[Partition[_]]] = {
    val typedPartitions = inputPartitions.asInstanceOf[Seq[Partition[(S, A)]]]
    executePerPartition(typedPartitions, Stage.sortLocal[S, A](sortBy.meta.ordering))
  }

  /**
   * Per-partition groupByKey. Input partitions are already key-hashed, so this is the same
   * operator as the shuffle-bypass path.
   */
  private def executeGroupByKeyOperation(
      inputPartitions: Seq[Partition[_]],
  ): F[Seq[Partition[_]]] = {
    val typedPartitions = inputPartitions.asInstanceOf[Seq[Partition[(Any, Any)]]]
    executePerPartition(typedPartitions, Stage.groupByKeyLocal[Any, Any])
  }

  /**
   * Per-partition reduceByKey. Named erasure boundary: stage transport erases record types, but V
   * is known from ReduceByKeyWideOp; only values reach the typed reduce function.
   */
  private def executeReduceByKeyOperation[V](
      reduceByKey: ReduceByKeyWideOp[V],
      inputPartitions: Seq[Partition[_]],
  ): F[Seq[Partition[_]]] = {
    val typedPartitions = inputPartitions.asInstanceOf[Seq[Partition[(Any, V)]]]
    executePerPartition(
      typedPartitions,
      Stage.reduceByKeyLocal[Any, V](reduceByKey.meta.reduceFunc),
    )
  }

  /**
   * Per-partition cogroup of co-located left/right shuffle partitions.
   */
  private def executeCoGroupOperation(
      stageInfo: StageBuilder.StageInfo,
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    val (leftShuffleId, rightShuffleId, numPartitions) =
      resolveBinaryShuffleInputs(stageInfo, stageToShuffleId, "Cogroup")
    val tasks = (0 until numPartitions).map { partitionId =>
      val leftData =
        shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data.toSeq
      val rightData =
        shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data.toSeq
      Task.createCogroupTask[Any, Any, Any](leftData, rightData)
    }
    scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
  }

  /**
   * Type-safe handler for repartition operations.
   */
  private def executeRepartitionOperation(
      inputPartitions: Seq[Partition[_]],
  ): F[Seq[Partition[_]]] = {
    Sync[F].delay {
      val typed = inputPartitions.asInstanceOf[Seq[Partition[(Any, Unit)]]]
      val out = typed.map { p => Partition(p.data.iterator.map(_._1).toList) }
      out
    }
  }

  /**
   * Executes a narrow transformation stage. Now generic for input (T) and output (U) types.
   */
  private def executeNarrowStage[A, B](
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[A]],
      @annotation.unused stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    executePerPartition(inputPartitions, stageInfo.stage.asInstanceOf[Stage[A, B]])
  }
