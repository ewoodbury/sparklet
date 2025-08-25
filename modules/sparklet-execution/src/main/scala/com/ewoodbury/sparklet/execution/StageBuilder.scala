package com.ewoodbury.sparklet.execution

import scala.collection.mutable

import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf, StageId}
import com.ewoodbury.sparklet.execution.{Operation, WideOp, WideOpMeta, WideOpKind}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.MutableDataStructures",
  ),
)

/**
 * Builds stage execution graphs from plans, handling both narrow transformations and shuffle
 * boundaries for wide transformations.
 */
object StageBuilder:
  /** Describes how a stage's output is partitioned. */
  final case class Partitioning(byKey: Boolean, numPartitions: Int)

  /**
   * Information about a stage in the execution graph.
   */
  case class StageInfo(
      id: StageId,
      stage: Stage[_, _],
      inputSources: Seq[InputSource], // What this stage reads from
      isShuffleStage: Boolean,
      shuffleOperation: Option[Plan[_]], // The original Plan operation for shuffle stages - will be replaced with WideOp in future
      outputPartitioning: Option[Partitioning],
  )

  /**
   * Represents where a stage gets its input data from.
   */
  sealed trait InputSource
  case class SourceInput(partitions: Seq[Partition[_]]) extends InputSource

  /**
   * Side tag to disambiguate multi-input wide operations (e.g., join/cogroup).
   */
  enum Side:
    case Left, Right

  /**
   * Unified shuffle input that can handle both single-input and multi-input shuffle operations.
   * The optional side parameter disambiguates multi-input operations (e.g., join/cogroup).
   */
  case class ShuffleInput(stageId: StageId, side: Option[Side], numPartitions: Int)
      extends InputSource

  /**
   * References the runtime output of a previously computed stage. Used for operations like union
   * that need to concatenate upstream results without reshuffling.
   */
  case class StageOutput(stageId: StageId) extends InputSource

  /**
   * Complete stage execution graph with dependencies.
   */
  case class StageGraph(
      stages: Map[StageId, StageInfo],
      dependencies: Map[StageId, Set[StageId]], // stage ID -> dependent stage IDs
      finalStageId: StageId,
  )

  /**
   * Mutable builder for accumulating operations during stage construction.
   * This will replace the current recursive approach with a more structured
   * accumulation of operations that can later be optimized and materialized.
   */
  private case class MutableStageBuilder(
      id: StageId,
      ops: Vector[Operation],
      inputSources: Seq[InputSource],
      isShuffle: Boolean,
      shuffleMeta: Option[Plan[_]], // The original Plan operation for shuffle stages
      outputPartitioning: Option[Partitioning],
  )

  // Per-build context to generate monotonically increasing stage IDs without global/shared state
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final case class BuildContext(var nextId: Int):
    def freshId(): StageId =
      val id = StageId(nextId)
      nextId = nextId + 1
      id

  /**
   * Builds a complete stage graph from a plan, handling shuffle boundaries.
   */
  def buildStageGraph[A](plan: Plan[A]): StageGraph = {
    val ctx = BuildContext(0)
    val stageMap = mutable.Map[StageId, StageInfo]()
    val dependencies = mutable.Map[StageId, mutable.Set[StageId]]()

    val finalStageId = buildStagesRecursive(ctx, plan, stageMap, dependencies)

    StageGraph(
      stageMap.toMap,
      dependencies.map { case (k, v) => k -> v.toSet }.toMap,
      finalStageId,
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.RedundantAsInstanceOf"))
  /**
   * Recursively builds stages, handling narrow transformations and shuffle boundaries. Returns the
   * stage ID that produces the final result.
   */
  private def buildStagesRecursive[A](
      ctx: BuildContext,
      plan: Plan[A],
      stageMap: mutable.Map[StageId, StageInfo],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): StageId = {
    plan match {
      // Base case: data source
      case source: Plan.Source[_] =>
        val stageId = ctx.freshId()
        val stage = Stage.SingleOpStage[Any, Any](identity)
        stageMap(stageId) = StageInfo(
          id = stageId,
          stage = stage,
          inputSources = Seq(SourceInput(source.partitions)),
          isShuffleStage = false,
          shuffleOperation = None,
          outputPartitioning =
            Some(Partitioning(byKey = false, numPartitions = source.partitions.size)),
        )
        stageId

      // Narrow transformations - can be chained together
      case Plan.MapOp(sourcePlan, f) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendWithMetadata(
          ctx,
          sourceStageId,
          Stage.map(f).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
          preservePartitioning,
        )

      case Plan.FilterOp(sourcePlan, p) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendWithMetadata(
          ctx,
          sourceStageId,
          Stage.filter(p).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
          preservePartitioning,
        )

      case Plan.FlatMapOp(sourcePlan, f) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendWithMetadata(
          ctx,
          sourceStageId,
          Stage.flatMap(f).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
          preservePartitioning,
        )

      case Plan.DistinctOp(sourcePlan) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendWithMetadata(
          ctx,
          sourceStageId,
          Stage.distinct.asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
          preservePartitioning,
        )

      case Plan.KeysOp(sourcePlan) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendWithMetadata(
          ctx,
          sourceStageId,
          Stage.keys.asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
          clearKeyedPreserveCount,
        )

      case Plan.ValuesOp(sourcePlan) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendWithMetadata(
          ctx,
          sourceStageId,
          Stage.values.asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
          clearKeyedPreserveCount,
        )

      case Plan.MapValuesOp(sourcePlan, f) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendWithMetadata(
          ctx,
          sourceStageId,
          Stage.mapValues(f).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
          preservePartitioning,
        )

      case Plan.FilterKeysOp(sourcePlan, p) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendWithMetadata(
          ctx,
          sourceStageId,
          Stage.filterKeys(p).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
          preservePartitioning,
        )

      case Plan.FilterValuesOp(sourcePlan, p) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendWithMetadata(
          ctx,
          sourceStageId,
          Stage.filterValues(p).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
          preservePartitioning,
        )

      case Plan.FlatMapValuesOp(sourcePlan, f) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendWithMetadata(
          ctx,
          sourceStageId,
          Stage.flatMapValues(f).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
          preservePartitioning,
        )

      case Plan.MapPartitionsOp(sourcePlan, f) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendWithMetadata(
          ctx,
          sourceStageId,
          Stage.mapPartitions(f).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
          preservePartitioning,
        )

      case Plan.UnionOp(left, right) =>
        val leftStageId = buildStagesRecursive(ctx, left, stageMap, dependencies)
        val rightStageId = buildStagesRecursive(ctx, right, stageMap, dependencies)

        // Union creates a new narrow stage that reads outputs from both input stages
        val unionStageId = ctx.freshId()
        val unionStage = Stage.SingleOpStage[Any, Any](identity)

        // Correct behavior: explicitly read from the outputs of the left and right stages
        // rather than re-reading their original sources.
        stageMap(unionStageId) = StageInfo(
          id = unionStageId,
          stage = unionStage,
          inputSources = Seq(StageOutput(leftStageId), StageOutput(rightStageId)),
          isShuffleStage = false,
          shuffleOperation = None,
          outputPartitioning = None,
        )

        dependencies.getOrElseUpdate(unionStageId, mutable.Set.empty) += leftStageId
        dependencies.getOrElseUpdate(unionStageId, mutable.Set.empty) += rightStageId
        unionStageId

      // --- Wide Transformations (create shuffle boundaries) ---
      case groupByKey: Plan.GroupByKeyOp[_, _] =>
        val sourceStageId = buildStagesRecursive(ctx, groupByKey.source, stageMap, dependencies)
        val src = stageMap(sourceStageId)
        val defaultN = SparkletConf.get.defaultShufflePartitions

        if (Operation.canBypassShuffle(groupByKey, src.outputPartitioning, SparkletConf.get)) then
          extendWithMetadata(
            ctx,
            sourceStageId,
            Stage.groupByKeyLocal.asInstanceOf[Stage[Any, Any]],
            stageMap,
            dependencies,
            preservePartitioning,
          )
        else
          val opMeta = WideOpMeta(
            kind = WideOpKind.GroupByKey,
            numPartitions = defaultN
          )
          buildWideStage(ctx, Seq(sourceStageId), opMeta, stageMap, dependencies, Some(groupByKey))

      case reduceByKey: Plan.ReduceByKeyOp[_, _] =>
        val sourceStageId = buildStagesRecursive(ctx, reduceByKey.source, stageMap, dependencies)
        val src = stageMap(sourceStageId)
        val defaultN = SparkletConf.get.defaultShufflePartitions

        if (Operation.canBypassShuffle(reduceByKey, src.outputPartitioning, SparkletConf.get)) then
          extendWithMetadata(
            ctx,
            sourceStageId,
            Stage
              .reduceByKeyLocal(reduceByKey.reduceFunc.asInstanceOf[(Any, Any) => Any])
              .asInstanceOf[Stage[Any, Any]],
            stageMap,
            dependencies,
            preservePartitioning,
          )
        else
          val opMeta = WideOpMeta(
            kind = WideOpKind.ReduceByKey,
            numPartitions = defaultN,
            reduceFunc = Some(reduceByKey.reduceFunc.asInstanceOf[(Any, Any) => Any])
          )
          buildWideStage(ctx, Seq(sourceStageId), opMeta, stageMap, dependencies, Some(reduceByKey))

      case sortBy: Plan.SortByOp[_, _] =>
        val sourceStageId = buildStagesRecursive(ctx, sortBy.source, stageMap, dependencies)
        val n = SparkletConf.get.defaultShufflePartitions
        val opMeta = WideOpMeta(
          kind = WideOpKind.SortBy,
          numPartitions = n,
          keyFunc = Some(sortBy.keyFunc.asInstanceOf[Any => Any])
        )
        buildWideStage(ctx, Seq(sourceStageId), opMeta, stageMap, dependencies, Some(sortBy))

      case pby: Plan.PartitionByOp[_, _] =>
        val sourceStageId = buildStagesRecursive(ctx, pby.source, stageMap, dependencies)
        val src = stageMap(sourceStageId)

        if (Operation.canBypassShuffle(pby, src.outputPartitioning, SparkletConf.get)) then
          extendWithMetadata(
            ctx,
            sourceStageId,
            Stage.SingleOpStage[Any, Any](identity), // No-op since already correctly partitioned
            stageMap,
            dependencies,
            preservePartitioning,
          )
        else
          val opMeta = WideOpMeta(
            kind = WideOpKind.PartitionBy,
            numPartitions = pby.numPartitions
          )
          buildWideStage(ctx, Seq(sourceStageId), opMeta, stageMap, dependencies, Some(pby))

      case rep: Plan.RepartitionOp[_] =>
        val sourceStageId = buildStagesRecursive(ctx, rep.source, stageMap, dependencies)
        val src = stageMap(sourceStageId)

        if (Operation.canBypassShuffle(rep, src.outputPartitioning, SparkletConf.get)) then
          extendWithMetadata(
            ctx,
            sourceStageId,
            Stage.SingleOpStage[Any, Any](identity), // No-op since already correctly partitioned
            stageMap,
            dependencies,
            preservePartitioning,
          )
        else
          val opMeta = WideOpMeta(
            kind = WideOpKind.Repartition,
            numPartitions = rep.numPartitions
          )
          buildWideStage(ctx, Seq(sourceStageId), opMeta, stageMap, dependencies, Some(rep))

      case coal: Plan.CoalesceOp[_] =>
        val sourceStageId = buildStagesRecursive(ctx, coal.source, stageMap, dependencies)
        val src = stageMap(sourceStageId)

        if (Operation.canBypassShuffle(coal, src.outputPartitioning, SparkletConf.get)) then
          extendWithMetadata(
            ctx,
            sourceStageId,
            Stage.SingleOpStage[Any, Any](identity), // No-op since already correctly partitioned
            stageMap,
            dependencies,
            preservePartitioning,
          )
        else
          val opMeta = WideOpMeta(
            kind = WideOpKind.Coalesce,
            numPartitions = coal.numPartitions
          )
          buildWideStage(ctx, Seq(sourceStageId), opMeta, stageMap, dependencies, Some(coal))

      case joinOp: Plan.JoinOp[_, _, _] =>
        val leftStageId = buildStagesRecursive(ctx, joinOp.left, stageMap, dependencies)
        val rightStageId = buildStagesRecursive(ctx, joinOp.right, stageMap, dependencies)

        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val opMeta = WideOpMeta(
          kind = WideOpKind.Join,
          numPartitions = numPartitions,
          joinStrategy = joinOp.joinStrategy,
          sides = Seq(Side.Left, Side.Right)
        )

        buildWideStage(ctx, Seq(leftStageId, rightStageId), opMeta, stageMap, dependencies, Some(joinOp))

      case cogroupOp: Plan.CoGroupOp[_, _, _] =>
        val leftStageId = buildStagesRecursive(ctx, cogroupOp.left, stageMap, dependencies)
        val rightStageId = buildStagesRecursive(ctx, cogroupOp.right, stageMap, dependencies)

        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val opMeta = WideOpMeta(
          kind = WideOpKind.CoGroup,
          numPartitions = numPartitions,
          sides = Seq(Side.Left, Side.Right)
        )

        buildWideStage(ctx, Seq(leftStageId, rightStageId), opMeta, stageMap, dependencies, Some(cogroupOp))
    }
  }

  /**
   * Extends an existing stage with a new narrow transformation, or creates a new stage if the
   * existing stage is a shuffle stage.
   */
  private def extendStageOrCreateNew(
      ctx: BuildContext,
      sourceStageId: StageId,
      newOperation: Stage[Any, Any],
      stageMap: mutable.Map[StageId, StageInfo],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): StageId = {
    val sourceStage = stageMap(sourceStageId)

    if (sourceStage.isShuffleStage) {
      // Can't extend a shuffle stage, create a new one
      val newStageId = ctx.freshId()

      // Set up input sources to read from the runtime output of the source stage
      val inputSources = Seq(StageOutput(sourceStageId))

      stageMap(newStageId) = StageInfo(
        id = newStageId,
        stage = newOperation,
        inputSources = inputSources,
        isShuffleStage = false,
        shuffleOperation = None,
        outputPartitioning = sourceStage.outputPartitioning,
      )

      // Add dependency: new stage depends on source stage
      dependencies.getOrElseUpdate(newStageId, mutable.Set.empty) += sourceStageId

      newStageId
    } else {
      // Extend the existing stage by chaining operations
      val chainedStage = Stage.ChainedStage(
        sourceStage.stage.asInstanceOf[Stage[Any, Any]],
        newOperation,
      )
      stageMap(sourceStageId) = sourceStage.copy(stage = chainedStage)
      sourceStageId
    }
  }

  // --- Metadata helpers ---

  /**
   * Centralized function for propagating partitioning metadata based on operation semantics.
   * Rules:
   * - Map/Filter/FlatMap/MapPartitions: preserve existing partitioning
   * - Keys/Values/MapValues/FilterKeys/FilterValues/FlatMapValues: preserve numPartitions but may clear byKey
   * - Distinct: clears byKey (unless previous was byKey - define explicitly)
   * - Wide operations: set byKey and numPartitions based on operation type
   */
  private def updatePartitioning(prev: Option[Partitioning], op: Operation): Option[Partitioning] = {
    op match {
      // Narrow operations that preserve partitioning completely
      case _: MapOp[_, _] | _: FilterOp[_] | _: FlatMapOp[_, _] | _: MapPartitionsOp[_, _] =>
        prev

      // Operations that preserve partition count but may affect key awareness
      case _: KeysOp[_, _] =>
        // Keys keeps byKey=false because output is key-only set
        prev.map(p => p.copy(byKey = false))

      case _: ValuesOp[_, _] =>
        // Values clears byKey because output is value-only
        prev.map(p => p.copy(byKey = false))

      case _: MapValuesOp[_, _, _] | _: FilterKeysOp[_, _] | _: FilterValuesOp[_, _] | _: FlatMapValuesOp[_, _, _] =>
        // These preserve the partitioning structure
        prev

      case _: DistinctOp =>
        // Distinct clears byKey unless previous was byKey (preserves key-based partitioning for key-value data)
        prev.map(p => if (p.byKey) p else p.copy(byKey = false))

      // Wide operations that create new partitioning
      case gbk: GroupByKeyOp[_, _] =>
        Some(Partitioning(byKey = true, numPartitions = gbk.numPartitions))

      case rbk: ReduceByKeyOp[_, _] =>
        Some(Partitioning(byKey = true, numPartitions = rbk.numPartitions))

      case sb: SortByOp[_, _] =>
        // SortBy creates key-based partitioning but doesn't guarantee byKey for output
        Some(Partitioning(byKey = false, numPartitions = sb.numPartitions))

      case pby: PartitionByOp[_, _] =>
        Some(Partitioning(byKey = true, numPartitions = pby.numPartitions))

      case rep: RepartitionOp[_] =>
        Some(Partitioning(byKey = false, numPartitions = rep.numPartitions))

      case coal: CoalesceOp[_] =>
        Some(Partitioning(byKey = false, numPartitions = coal.numPartitions))

      case join: JoinOp[_, _, _] =>
        Some(Partitioning(byKey = true, numPartitions = join.numPartitions))

      case cogroup: CoGroupOp[_, _, _] =>
        Some(Partitioning(byKey = true, numPartitions = cogroup.numPartitions))
    }
  }

  // Legacy helper functions - kept for backward compatibility during transition
  private def preservePartitioning(prev: Option[Partitioning]): Option[Partitioning] = prev

  private def clearKeyedPreserveCount(prev: Option[Partitioning]): Option[Partitioning] =
    prev.map(p => p.copy(byKey = false))

  /**
   * Extends the stage graph with a new narrow operation while updating output partitioning
   * metadata according to the provided update function.
   */
  private def extendWithMetadata(
      ctx: BuildContext,
      sourceStageId: StageId,
      newOperation: Stage[Any, Any],
      stageMap: mutable.Map[StageId, StageInfo],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
      update: Option[Partitioning] => Option[Partitioning],
  ): StageId = {
    val sid = extendStageOrCreateNew(ctx, sourceStageId, newOperation, stageMap, dependencies)
    val info = stageMap(sid)
    val newMeta = update(info.outputPartitioning)
    stageMap(sid) = info.copy(outputPartitioning = newMeta)
    sid
  }

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  /**
   * Encapsulates wide operation handling by creating a shuffle stage with proper metadata.
   * This is the main entry point for creating shuffle stages in the new architecture.
   */
  private def buildWideStage(
      ctx: BuildContext,
      upstreamIds: Seq[StageId],
      opMeta: WideOpMeta,
      stageMap: mutable.Map[StageId, StageInfo],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
      shuffleOperation: Option[Plan[_]] = None, // For backward compatibility
  ): StageId = {
    val shuffleStageId = ctx.freshId()

    // Create a placeholder stage for shuffle operations
    val shuffleStage =
      Stage.SingleOpStage[Any, Any](identity) // Will be replaced with actual shuffle logic

    // Set up shuffle input sources based on operation type and upstream stages
    val shuffleInputSources = opMeta.kind match {
      case WideOpKind.Join | WideOpKind.CoGroup =>
        // Multi-input operations need sides
        require(upstreamIds.length == 2, s"${opMeta.kind} requires exactly 2 upstream stages")
        require(opMeta.sides.length == 2, s"${opMeta.kind} requires exactly 2 sides")
        Seq(
          ShuffleInput(upstreamIds(0), Some(opMeta.sides(0)), opMeta.numPartitions),
          ShuffleInput(upstreamIds(1), Some(opMeta.sides(1)), opMeta.numPartitions)
        )
      case _ =>
        // Single-input operations
        require(upstreamIds.length == 1, s"${opMeta.kind} requires exactly 1 upstream stage")
        Seq(ShuffleInput(upstreamIds.head, None, opMeta.numPartitions))
    }

    // Create the appropriate WideOp instance
    val wideOp: WideOp = opMeta.kind match {
      case WideOpKind.GroupByKey => GroupByKeyWideOp(opMeta)
      case WideOpKind.ReduceByKey => ReduceByKeyWideOp(opMeta)
      case WideOpKind.SortBy => SortByWideOp(opMeta)
      case WideOpKind.PartitionBy => PartitionByWideOp(opMeta)
      case WideOpKind.Repartition => RepartitionWideOp(opMeta)
      case WideOpKind.Coalesce => CoalesceWideOp(opMeta)
      case WideOpKind.Join => JoinWideOp(opMeta)
      case WideOpKind.CoGroup => CoGroupWideOp(opMeta)
    }

    // Determine output partitioning based on operation type
    val outputPartitioning = opMeta.kind match {
      case WideOpKind.GroupByKey | WideOpKind.ReduceByKey | WideOpKind.PartitionBy | WideOpKind.Join | WideOpKind.CoGroup =>
        Some(Partitioning(byKey = true, numPartitions = opMeta.numPartitions))
      case WideOpKind.SortBy =>
        Some(Partitioning(byKey = false, numPartitions = opMeta.numPartitions))
      case WideOpKind.Repartition | WideOpKind.Coalesce =>
        Some(Partitioning(byKey = false, numPartitions = opMeta.numPartitions))
    }

    stageMap(shuffleStageId) = StageInfo(
      id = shuffleStageId,
      stage = shuffleStage,
      inputSources = shuffleInputSources,
      isShuffleStage = true,
      shuffleOperation = shuffleOperation, // Keep original Plan for backward compatibility
      outputPartitioning = outputPartitioning,
    )

    // Add dependencies for all upstream stages
    upstreamIds.foreach { upstreamId =>
      dependencies.getOrElseUpdate(shuffleStageId, mutable.Set.empty) += upstreamId
    }

    shuffleStageId
  }

  /**
   * Creates a new shuffle stage that depends on the source stage.
   * @deprecated Use buildWideStage instead - this is kept for backward compatibility during transition.
   */
  private def createShuffleStage(
      ctx: BuildContext,
      sourceStageId: StageId,
      @annotation.unused operationType: String,
      stageMap: mutable.Map[StageId, StageInfo],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
      @annotation.unused operation: Option[Any] = None,
      shuffleOperation: Option[Plan[_]] = None,
      numPartitions: Int = SparkletConf.get.defaultShufflePartitions,
      resultPartitioning: Option[Partitioning] = None,
  ): StageId = {
    // Create a temporary WideOpMeta for backward compatibility
    val tempMeta = WideOpMeta(
      kind = WideOpKind.GroupByKey, // Default fallback
      numPartitions = numPartitions
    )

    // Convert Plan to WideOp for new architecture
    val wideOp = shuffleOperation.flatMap { plan =>
      plan match {
        case gbk: Plan.GroupByKeyOp[_, _] => Some(GroupByKeyWideOp(tempMeta.copy(kind = WideOpKind.GroupByKey)))
        case rbk: Plan.ReduceByKeyOp[_, _] => Some(ReduceByKeyWideOp(tempMeta.copy(kind = WideOpKind.ReduceByKey, reduceFunc = Some(rbk.reduceFunc.asInstanceOf[(Any, Any) => Any]))))
        case sb: Plan.SortByOp[_, _] => Some(SortByWideOp(tempMeta.copy(kind = WideOpKind.SortBy, keyFunc = Some(sb.keyFunc.asInstanceOf[Any => Any]))))
        case pby: Plan.PartitionByOp[_, _] => Some(PartitionByWideOp(tempMeta.copy(kind = WideOpKind.PartitionBy)))
        case rep: Plan.RepartitionOp[_] => Some(RepartitionWideOp(tempMeta.copy(kind = WideOpKind.Repartition)))
        case coal: Plan.CoalesceOp[_] => Some(CoalesceWideOp(tempMeta.copy(kind = WideOpKind.Coalesce)))
        case join: Plan.JoinOp[_, _, _] => Some(JoinWideOp(tempMeta.copy(kind = WideOpKind.Join, joinStrategy = join.joinStrategy)))
        case cogroup: Plan.CoGroupOp[_, _, _] => Some(CoGroupWideOp(tempMeta.copy(kind = WideOpKind.CoGroup)))
        case _ => None
      }
    }

    buildWideStage(ctx, Seq(sourceStageId), tempMeta, stageMap, dependencies)
  }

  /**
   * Legacy method for backward compatibility with current Executor. Will be removed once
   * DAGScheduler is fully integrated.
   */
  def buildStages[A](plan: Plan[A]): Seq[(Plan.Source[_], Stage[_, A])] = {
    // Check if plan contains shuffle operations
    if (containsShuffleOperations(plan)) {
      throw new UnsupportedOperationException(
        "Plan contains shuffle operations - use DAGScheduler instead of legacy buildStages",
      )
    }

    // For narrow-only plans, use the old logic
    buildStagesRecursiveOld(plan)
  }

  private def containsShuffleOperations(plan: Plan[_]): Boolean = plan match {
    case _: Plan.GroupByKeyOp[_, _] | _: Plan.ReduceByKeyOp[_, _] | _: Plan.SortByOp[_, _] |
        _: Plan.JoinOp[_, _, _] | _: Plan.CoGroupOp[_, _, _] =>
      true
    case Plan.MapOp(source, _) => containsShuffleOperations(source)
    case Plan.FilterOp(source, _) => containsShuffleOperations(source)
    case Plan.FlatMapOp(source, _) => containsShuffleOperations(source)
    case Plan.DistinctOp(source) => containsShuffleOperations(source)
    case Plan.KeysOp(source) => containsShuffleOperations(source)
    case Plan.ValuesOp(source) => containsShuffleOperations(source)
    case Plan.MapValuesOp(source, _) => containsShuffleOperations(source)
    case Plan.FilterKeysOp(source, _) => containsShuffleOperations(source)
    case Plan.FilterValuesOp(source, _) => containsShuffleOperations(source)
    case Plan.FlatMapValuesOp(source, _) => containsShuffleOperations(source)
    case Plan.UnionOp(left, right) =>
      containsShuffleOperations(left) || containsShuffleOperations(right)
    case _ => false
  }

  // Legacy implementation for narrow transformations only
  private def buildStagesRecursiveOld[A](plan: Plan[A]): Seq[(Plan.Source[_], Stage[_, A])] = {
    plan match {
      case source: Plan.Source[A] =>
        Seq((source, Stage.SingleOpStage[A, A](identity)))

      case Plan.MapOp(sourcePlan, f) =>
        val stages = buildStagesRecursiveOld(sourcePlan)
        extendLastStage(stages, Stage.map(f))

      case Plan.FilterOp(sourcePlan, p) =>
        val stages = buildStagesRecursiveOld(sourcePlan)
        extendLastStage(stages, Stage.filter(p))

      case Plan.FlatMapOp(sourcePlan, f) =>
        val stages = buildStagesRecursiveOld(sourcePlan)
        extendLastStage(stages, Stage.flatMap(f))

      case Plan.DistinctOp(sourcePlan) =>
        val stages = buildStagesRecursiveOld(sourcePlan)
        extendLastStage(stages, Stage.distinct)

      case Plan.KeysOp(sourcePlan) =>
        val stages = buildStagesRecursiveOld(sourcePlan)
        extendLastStage(stages, Stage.keys)

      case Plan.ValuesOp(sourcePlan) =>
        val stages = buildStagesRecursiveOld(sourcePlan)
        extendLastStage(stages, Stage.values)

      case Plan.MapValuesOp(sourcePlan, f) =>
        val stages = buildStagesRecursiveOld(sourcePlan)
        extendLastStage(
          stages.asInstanceOf[Seq[(Plan.Source[_], Stage[_, Any])]],
          Stage.mapValues(f).asInstanceOf[Stage[Any, A]],
        )

      case Plan.FilterKeysOp(sourcePlan, p) =>
        val stages = buildStagesRecursiveOld(sourcePlan)
        extendLastStage(
          stages.asInstanceOf[Seq[(Plan.Source[_], Stage[_, Any])]],
          Stage.filterKeys(p).asInstanceOf[Stage[Any, A]],
        )

      case Plan.FilterValuesOp(sourcePlan, p) =>
        val stages = buildStagesRecursiveOld(sourcePlan)
        extendLastStage(
          stages.asInstanceOf[Seq[(Plan.Source[_], Stage[_, Any])]],
          Stage.filterValues(p).asInstanceOf[Stage[Any, A]],
        )

      case Plan.FlatMapValuesOp(sourcePlan, f) =>
        val stages = buildStagesRecursiveOld(sourcePlan)
        extendLastStage(
          stages.asInstanceOf[Seq[(Plan.Source[_], Stage[_, Any])]],
          Stage.flatMapValues(f).asInstanceOf[Stage[Any, A]],
        )

      case Plan.MapPartitionsOp(sourcePlan, f) =>
        val stages = buildStagesRecursiveOld(sourcePlan)
        extendLastStage(
          stages.asInstanceOf[Seq[(Plan.Source[_], Stage[_, Any])]],
          Stage.mapPartitions(f).asInstanceOf[Stage[Any, A]],
        )

      case Plan.UnionOp(left, right) =>
        buildStagesRecursiveOld(left) ++ buildStagesRecursiveOld(right)

      case _ =>
        throw new UnsupportedOperationException(s"Legacy stage building for $plan not supported")
    }
  }

  private def extendLastStage[A, B, C](
      stages: Seq[(Plan.Source[_], Stage[_, B])],
      newStage: Stage[B, C],
  ): Seq[(Plan.Source[_], Stage[_, C])] = {
    val initStages = stages.dropRight(1).asInstanceOf[Seq[(Plan.Source[_], Stage[_, C])]]
    val (source, lastStage) =
      stages.lastOption.getOrElse(throw new IllegalStateException("No stages to extend"))

    initStages :+ (
      source,
      Stage
        .ChainedStage(lastStage.asInstanceOf[Stage[Any, B]], newStage)
        .asInstanceOf[Stage[Any, C]],
    )
  }
