package com.ewoodbury.sparklet.execution

import scala.collection.mutable

import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf, StageId}

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
   * Mutable builder for accumulating operations during stage construction
   */
  private case class MutableStageBuilder(
      id: StageId,
      ops: Vector[Operation],
      inputSources: Seq[InputSource],
      isShuffle: Boolean,
      shuffleMeta: Option[WideOp], // Use WideOp instead of Plan for better structure
      originalPlan: Option[Plan[_]], // Keep original Plan for backward compatibility
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
   * Builds a complete stage graph from a plan using the unified builder approach.
   * This replaces the old recursive approach with a more structured operation accumulation.
   */
  def buildStageGraph[A](plan: Plan[A]): StageGraph = {
    val ctx = BuildContext(0)
    val builderMap = mutable.Map[StageId, MutableStageBuilder]()
    val dependencies = mutable.Map[StageId, mutable.Set[StageId]]()

    val (finalStageId, _) = buildUnified(ctx, plan, builderMap, dependencies)

    // Convert MutableStageBuilder instances to StageInfo instances
    val stageMap = mutable.Map[StageId, StageInfo]()
    builderMap.foreachEntry { (stageId, builder) =>
      val stage = if (builder.isShuffle) {
        // For shuffle stages, create placeholder stage (will be replaced by actual shuffle logic)
        Stage.SingleOpStage[Any, Any](identity)
      } else {
        // For narrow stages, materialize the operation vector into a concrete stage
        // If there are no operations (e.g., source stages), create an identity stage
        if (builder.ops.isEmpty) {
          Stage.SingleOpStage[Any, Any](identity)
        } else {
          materialize(builder.ops)
        }
      }

      stageMap(stageId) = StageInfo(
        id = stageId,
        stage = stage,
        inputSources = builder.inputSources,
        isShuffleStage = builder.isShuffle,
        shuffleOperation = builder.originalPlan,
        outputPartitioning = builder.outputPartitioning,
      )
    }

    StageGraph(
      stageMap.toMap,
      dependencies.map { case (k, v) => k -> v.toSet }.toMap,
      finalStageId,
    )
  }

  /**
   * Materializes a vector of operations into a concrete Stage form.
   * This builds a left fold producing existing Stage.* operations for minimal churn.
   */
  private def materialize(ops: Vector[Operation]): Stage[Any, Any] = {
    require(ops.nonEmpty, "Cannot materialize empty operation vector")

    // Start with identity stage and fold operations from left to right
    val startStage: Stage[Any, Any] = Stage.SingleOpStage[Any, Any](identity)

    ops.foldLeft(startStage) { (stage, op) =>
      op match {
        case MapOp(f) => Stage.ChainedStage(stage, Stage.map(f.asInstanceOf[Any => Any]))
        case FilterOp(p) => Stage.ChainedStage(stage, Stage.filter(p.asInstanceOf[Any => Boolean]))
        case FlatMapOp(f) => Stage.ChainedStage(stage, Stage.flatMap(f.asInstanceOf[Any => IterableOnce[Any]]))
        case DistinctOp() => Stage.ChainedStage(stage, Stage.distinct.asInstanceOf[Stage[Any, Any]])
        case KeysOp() => Stage.ChainedStage(stage, Stage.keys.asInstanceOf[Stage[Any, Any]])
        case ValuesOp() => Stage.ChainedStage(stage, Stage.values.asInstanceOf[Stage[Any, Any]])
        case MapValuesOp(f) => Stage.ChainedStage(stage, Stage.mapValues(f.asInstanceOf[Any => Any]).asInstanceOf[Stage[Any, Any]])
        case FilterKeysOp(p) => Stage.ChainedStage(stage, Stage.filterKeys(p.asInstanceOf[Any => Boolean]).asInstanceOf[Stage[Any, Any]])
        case FilterValuesOp(p) => Stage.ChainedStage(stage, Stage.filterValues(p.asInstanceOf[Any => Boolean]).asInstanceOf[Stage[Any, Any]])
        case FlatMapValuesOp(f) => Stage.ChainedStage(stage, Stage.flatMapValues(f.asInstanceOf[Any => IterableOnce[Any]]).asInstanceOf[Stage[Any, Any]])
        case MapPartitionsOp(f) => Stage.ChainedStage(stage, Stage.mapPartitions(f.asInstanceOf[Iterator[Any] => Iterator[Any]]))
        case _ => throw new UnsupportedOperationException(s"Cannot materialize wide operation: $op")
      }
    }
  }

  /**
   * Unified recursive builder that accumulates operations in MutableStageBuilder instances.
   * This replaces the old recursive approach with operation accumulation for better optimization.
   * Returns a tuple of (stageId, originalPlan) to preserve shuffle operation info.
   */
  private def buildUnified[A](
      ctx: BuildContext,
      plan: Plan[A],
      builderMap: mutable.Map[StageId, MutableStageBuilder],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) = {
    plan match {
      // Base case: data source
      case source: Plan.Source[_] =>
        val stageId = ctx.freshId()
        builderMap(stageId) = MutableStageBuilder(
          id = stageId,
          ops = Vector.empty[Operation], // No operations for source
          inputSources = Seq(SourceInput(source.partitions)),
          isShuffle = false,
          shuffleMeta = None,
          originalPlan = Some(source),
          outputPartitioning = Some(Partitioning(byKey = false, numPartitions = source.partitions.size)),
        )
        (stageId, Some(source))

      // Narrow transformations - accumulate operations or create new stages
      case Plan.MapOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildUnified(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, MapOp(f), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.FilterOp(sourcePlan, p) =>
        val (sourceStageId, _) = buildUnified(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, FilterOp(p), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.FlatMapOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildUnified(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, FlatMapOp(f), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.DistinctOp(sourcePlan) =>
        val (sourceStageId, _) = buildUnified(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, DistinctOp(), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.KeysOp(sourcePlan) =>
        val (sourceStageId, _) = buildUnified(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, KeysOp(), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.ValuesOp(sourcePlan) =>
        val (sourceStageId, _) = buildUnified(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, ValuesOp(), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.MapValuesOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildUnified(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, MapValuesOp(f), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.FilterKeysOp(sourcePlan, p) =>
        val (sourceStageId, _) = buildUnified(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, FilterKeysOp(p), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.FilterValuesOp(sourcePlan, p) =>
        val (sourceStageId, _) = buildUnified(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, FilterValuesOp(p), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.FlatMapValuesOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildUnified(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, FlatMapValuesOp(f), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.MapPartitionsOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildUnified(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, MapPartitionsOp(f), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.UnionOp(left, right) =>
        val (leftStageId, _) = buildUnified(ctx, left, builderMap, dependencies)
        val (rightStageId, _) = buildUnified(ctx, right, builderMap, dependencies)

        // Union creates a new narrow stage that reads outputs from both input stages
        val unionStageId = ctx.freshId()
        builderMap(unionStageId) = MutableStageBuilder(
          id = unionStageId,
          ops = Vector.empty[Operation], // No operations, just union of inputs
          inputSources = Seq(StageOutput(leftStageId), StageOutput(rightStageId)),
          isShuffle = false,
          shuffleMeta = None,
          originalPlan = Some(plan): Option[Plan[_]],
          outputPartitioning = None, // Union doesn't preserve partitioning
        )

        dependencies.getOrElseUpdate(unionStageId, mutable.Set.empty) += leftStageId
        dependencies.getOrElseUpdate(unionStageId, mutable.Set.empty) += rightStageId
        (unionStageId, Some(plan))

      // Wide transformations (create shuffle boundaries)
      case groupByKey: Plan.GroupByKeyOp[_, _] =>
        val (sourceStageId, _) = buildUnified(ctx, groupByKey.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)
        val defaultN = SparkletConf.get.defaultShufflePartitions

        if (Operation.canBypassShuffle(groupByKey, src.outputPartitioning, SparkletConf.get)) {
          val resultId = appendOperation(ctx, sourceStageId, GroupByKeyOp[Any, Any](defaultN), builderMap, dependencies)
          (resultId, Some(groupByKey))
        } else {
          val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), GroupByKeyWideOp(WideOpMeta(
            kind = WideOpKind.GroupByKey,
            numPartitions = defaultN
          )), builderMap, dependencies, Some(groupByKey))
          (shuffleId, Some(groupByKey))
        }

      case reduceByKey: Plan.ReduceByKeyOp[_, _] =>
        val (sourceStageId, _) = buildUnified(ctx, reduceByKey.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)
        val defaultN = SparkletConf.get.defaultShufflePartitions

        if (Operation.canBypassShuffle(reduceByKey, src.outputPartitioning, SparkletConf.get)) {
          val resultId = appendOperation(ctx, sourceStageId, ReduceByKeyOp[Any, Any](reduceByKey.reduceFunc.asInstanceOf[(Any, Any) => Any], defaultN), builderMap, dependencies)
          (resultId, Some(reduceByKey))
        } else {
          val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), ReduceByKeyWideOp(WideOpMeta(
            kind = WideOpKind.ReduceByKey,
            numPartitions = defaultN,
            reduceFunc = Some(reduceByKey.reduceFunc.asInstanceOf[(Any, Any) => Any])
          )), builderMap, dependencies, Some(reduceByKey))
          (shuffleId, Some(reduceByKey))
        }

      case sortBy: Plan.SortByOp[_, _] =>
        val (sourceStageId, _) = buildUnified(ctx, sortBy.source, builderMap, dependencies)
        val n = SparkletConf.get.defaultShufflePartitions
        val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), SortByWideOp(WideOpMeta(
          kind = WideOpKind.SortBy,
          numPartitions = n,
          keyFunc = Some(sortBy.keyFunc.asInstanceOf[Any => Any])
        )), builderMap, dependencies, Some(sortBy))
        (shuffleId, Some(sortBy))

      case pby: Plan.PartitionByOp[_, _] =>
        val (sourceStageId, _) = buildUnified(ctx, pby.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)

        if (Operation.canBypassShuffle(pby, src.outputPartitioning, SparkletConf.get)) {
          val resultId = appendOperation(ctx, sourceStageId, PartitionByOp[Any, Any](pby.numPartitions), builderMap, dependencies)
          (resultId, Some(pby))
        } else {
          val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), PartitionByWideOp(WideOpMeta(
            kind = WideOpKind.PartitionBy,
            numPartitions = pby.numPartitions
          )), builderMap, dependencies, Some(pby))
          (shuffleId, Some(pby))
        }

      case rep: Plan.RepartitionOp[_] =>
        val (sourceStageId, _) = buildUnified(ctx, rep.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)

        if (Operation.canBypassShuffle(rep, src.outputPartitioning, SparkletConf.get)) {
          val resultId = appendOperation(ctx, sourceStageId, RepartitionOp[Any](rep.numPartitions), builderMap, dependencies)
          (resultId, Some(rep))
        } else {
          val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), RepartitionWideOp(WideOpMeta(
            kind = WideOpKind.Repartition,
            numPartitions = rep.numPartitions
          )), builderMap, dependencies, Some(rep))
          (shuffleId, Some(rep))
        }

      case coal: Plan.CoalesceOp[_] =>
        val (sourceStageId, _) = buildUnified(ctx, coal.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)

        if (Operation.canBypassShuffle(coal, src.outputPartitioning, SparkletConf.get)) {
          val resultId = appendOperation(ctx, sourceStageId, CoalesceOp[Any](coal.numPartitions), builderMap, dependencies)
          (resultId, Some(coal))
        } else {
          val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), CoalesceWideOp(WideOpMeta(
            kind = WideOpKind.Coalesce,
            numPartitions = coal.numPartitions
          )), builderMap, dependencies, Some(coal))
          (shuffleId, Some(coal))
        }

      case joinOp: Plan.JoinOp[_, _, _] =>
        val (leftStageId, _) = buildUnified(ctx, joinOp.left, builderMap, dependencies)
        val (rightStageId, _) = buildUnified(ctx, joinOp.right, builderMap, dependencies)

        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val shuffleId = createShuffleStageUnified(ctx, Seq(leftStageId, rightStageId), JoinWideOp(WideOpMeta(
          kind = WideOpKind.Join,
          numPartitions = numPartitions,
          joinStrategy = joinOp.joinStrategy,
          sides = Seq(Side.Left, Side.Right)
        )), builderMap, dependencies, Some(joinOp))
        (shuffleId, Some(joinOp))

      case cogroupOp: Plan.CoGroupOp[_, _, _] =>
        val (leftStageId, _) = buildUnified(ctx, cogroupOp.left, builderMap, dependencies)
        val (rightStageId, _) = buildUnified(ctx, cogroupOp.right, builderMap, dependencies)

        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val shuffleId = createShuffleStageUnified(ctx, Seq(leftStageId, rightStageId), CoGroupWideOp(WideOpMeta(
          kind = WideOpKind.CoGroup,
          numPartitions = numPartitions,
          sides = Seq(Side.Left, Side.Right)
        )), builderMap, dependencies, Some(cogroupOp))
        (shuffleId, Some(cogroupOp))
    }
  }

  /**
   * Appends a narrow operation to an existing stage or creates a new stage if needed.
   * This handles the logic of when operations can be chained vs when new stages are required.
   * Returns the StageId that will produce the result (could be the source stage or a new one).
   */
  private def appendOperation(
      ctx: BuildContext,
      sourceStageId: StageId,
      op: Operation,
      builderMap: mutable.Map[StageId, MutableStageBuilder],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): StageId = {
    val sourceBuilder = builderMap(sourceStageId)

    if (sourceBuilder.isShuffle) {
      // Can't extend a shuffle stage, create a new narrow stage
      val newStageId = ctx.freshId()
      val newBuilder = MutableStageBuilder(
        id = newStageId,
        ops = Vector(op),
        inputSources = Seq(StageOutput(sourceStageId)),
        isShuffle = false,
        shuffleMeta = None,
        originalPlan = None,
        outputPartitioning = updatePartitioning(sourceBuilder.outputPartitioning, op),
      )
      builderMap(newStageId) = newBuilder

      // Add dependency: new stage depends on source stage
      dependencies.getOrElseUpdate(newStageId, mutable.Set.empty) += sourceStageId
      newStageId
    } else {
      // Check if we can chain this operation - only if the stage has a single producing path
      // For now, we chain if there are no multi-input sources (like Union)
      val canChain = sourceBuilder.inputSources.forall {
        case _: SourceInput => true
        case _: StageOutput => sourceBuilder.inputSources.length == 1
        case _: ShuffleInput => false
      }

      if (canChain && sourceBuilder.ops.nonEmpty) {
        // Extend the existing stage by appending the operation
        val updatedOps = sourceBuilder.ops :+ op
        val updatedBuilder = sourceBuilder.copy(
          ops = updatedOps,
          outputPartitioning = updatePartitioning(sourceBuilder.outputPartitioning, op)
        )
        builderMap(sourceStageId) = updatedBuilder
        sourceStageId
      } else {
        // Create a new stage with this operation
        val newStageId = ctx.freshId()
        val newBuilder = MutableStageBuilder(
          id = newStageId,
          ops = Vector(op),
          inputSources = sourceBuilder.inputSources,
          isShuffle = false,
          shuffleMeta = None,
          originalPlan = None,
          outputPartitioning = updatePartitioning(sourceBuilder.outputPartitioning, op),
        )
        builderMap(newStageId) = newBuilder

        // Copy dependencies from source stage
        sourceBuilder.inputSources.foreach {
          case StageOutput(upstreamId) =>
            dependencies.getOrElseUpdate(newStageId, mutable.Set.empty) += upstreamId
          case _ => // SourceInput and ShuffleInput don't create dependencies
        }

        newStageId
      }
    }
  }

  /**
   * Creates a shuffle stage using the unified builder approach.
   */
  @SuppressWarnings(Array("org.wartremover.warts.SeqApply", "org.wartremover.warts.IterableOps"))
  private def createShuffleStageUnified(
      ctx: BuildContext,
      upstreamIds: Seq[StageId],
      wideOp: WideOp,
      builderMap: mutable.Map[StageId, MutableStageBuilder],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
      originalPlan: Option[Plan[_]] = None,
  ): StageId = {
    val shuffleStageId = ctx.freshId()

    // Get metadata from the WideOp
    val meta = wideOp match {
      case GroupByKeyWideOp(m) => m
      case ReduceByKeyWideOp(m) => m
      case SortByWideOp(m) => m
      case PartitionByWideOp(m) => m
      case RepartitionWideOp(m) => m
      case CoalesceWideOp(m) => m
      case JoinWideOp(m) => m
      case CoGroupWideOp(m) => m
    }

    // Create shuffle input sources based on operation type
    val shuffleInputSources = meta.kind match {
      case WideOpKind.Join | WideOpKind.CoGroup =>
        // Multi-input operations need sides
        require(upstreamIds.length == 2, s"${meta.kind} requires exactly 2 upstream stages")
        require(meta.sides.length == 2, s"${meta.kind} requires exactly 2 sides")
        Seq(
          ShuffleInput(upstreamIds(0), Some(meta.sides(0)), meta.numPartitions),
          ShuffleInput(upstreamIds(1), Some(meta.sides(1)), meta.numPartitions)
        )
      case _ =>
        // Single-input operations
        require(upstreamIds.length == 1, s"${meta.kind} requires exactly 1 upstream stage")
        Seq(ShuffleInput(upstreamIds.head, None, meta.numPartitions))
    }

    // Determine output partitioning based on operation type
    val outputPartitioning = meta.kind match {
      case WideOpKind.GroupByKey | WideOpKind.ReduceByKey | WideOpKind.PartitionBy | WideOpKind.Join | WideOpKind.CoGroup =>
        Some(Partitioning(byKey = true, numPartitions = meta.numPartitions))
      case WideOpKind.SortBy =>
        Some(Partitioning(byKey = false, numPartitions = meta.numPartitions))
      case WideOpKind.Repartition | WideOpKind.Coalesce =>
        Some(Partitioning(byKey = false, numPartitions = meta.numPartitions))
    }

    builderMap(shuffleStageId) = MutableStageBuilder(
      id = shuffleStageId,
      ops = Vector.empty[Operation], // Shuffle stages don't have narrow operations
      inputSources = shuffleInputSources,
      isShuffle = true,
      shuffleMeta = Some(wideOp),
      originalPlan = originalPlan,
      outputPartitioning = outputPartitioning,
    )

    // Add dependencies for all upstream stages
    upstreamIds.foreach { upstreamId =>
      dependencies.getOrElseUpdate(shuffleStageId, mutable.Set.empty) += upstreamId
    }

    shuffleStageId
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
