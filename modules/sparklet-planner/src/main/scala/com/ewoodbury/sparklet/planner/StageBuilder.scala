package com.ewoodbury.sparklet.planner

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
   * Placeholder for stage operations in the planner.
   * The execution module will convert these to actual Stage objects.
   */
  sealed trait StageOperation
  case class Map(f: Any => Any) extends StageOperation
  case class Filter(predicate: Any => Boolean) extends StageOperation
  case class FlatMap(f: Any => IterableOnce[Any]) extends StageOperation
  case object Distinct extends StageOperation
  case object Keys extends StageOperation
  case object Values extends StageOperation
  case class MapValues(f: Any => Any) extends StageOperation
  case class FilterKeys(predicate: Any => Boolean) extends StageOperation
  case class FilterValues(predicate: Any => Boolean) extends StageOperation
  case class FlatMapValues(f: Any => IterableOnce[Any]) extends StageOperation
  case class MapPartitions(f: Iterator[Any] => Iterator[Any]) extends StageOperation
  case object GroupByKey extends StageOperation
  case class ReduceByKey(f: (Any, Any) => Any) extends StageOperation
  case class SortBy(f: Any => Any, ordering: Ordering[Any]) extends StageOperation
  case object Join extends StageOperation
  case object CoGroup extends StageOperation
  case class Repartition(numPartitions: Int) extends StageOperation
  case class Coalesce(numPartitions: Int) extends StageOperation
  case class PartitionBy(partitioner: com.ewoodbury.sparklet.runtime.api.Partitioner) extends StageOperation
  case object Union extends StageOperation

  /**
   * Information about a stage in the execution graph.
   */
  case class StageInfo(
      id: StageId,
      operations: List[StageOperation], // List of operations in this stage
      inputSources: Map[String, ShuffleId], // What shuffle data this stage reads from
      outputPartitions: Int,
      sourcePartitions: Option[Seq[Partition[_]]] = None, // For narrow-only stages
  )

  /**
   * Represents where a stage gets its input data from.
   */
  sealed trait InputSource
  case class SourceInput(partitions: Seq[Partition[_]]) extends InputSource
  case class ShuffleFrom(stageId: StageId, numPartitions: Int) extends InputSource

  /**
   * Side tag to disambiguate multi-input wide operations (e.g., join/cogroup).
   */
  enum Side:
    case Left, Right

  /**
   * Explicitly reference the upstream producing stage and which side it feeds for multi-input
   * shuffle operations. This avoids relying on heuristics to infer left/right shuffles.
   */
  case class TaggedShuffleFrom(stageId: StageId, side: Side, numPartitions: Int)
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
        val canSkipShuffle =
          src.outputPartitioning.exists(p => p.byKey && p.numPartitions == defaultN)
        if (canSkipShuffle) then
          extendWithMetadata(
            ctx,
            sourceStageId,
            Stage.groupByKeyLocal.asInstanceOf[Stage[Any, Any]],
            stageMap,
            dependencies,
            preservePartitioning,
          )
        else
          createShuffleStage(
            ctx,
            sourceStageId,
            "groupByKey",
            stageMap,
            dependencies,
            None,
            Some(groupByKey),
            numPartitions = defaultN,
            resultPartitioning = Some(Partitioning(byKey = true, numPartitions = defaultN)),
          )

      case reduceByKey: Plan.ReduceByKeyOp[_, _] =>
        val sourceStageId = buildStagesRecursive(ctx, reduceByKey.source, stageMap, dependencies)
        val src = stageMap(sourceStageId)
        val defaultN = SparkletConf.get.defaultShufflePartitions
        val canSkipShuffle =
          src.outputPartitioning.exists(p => p.byKey && p.numPartitions == defaultN)
        if (canSkipShuffle) then
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
          createShuffleStage(
            ctx,
            sourceStageId,
            "reduceByKey",
            stageMap,
            dependencies,
            Some(reduceByKey.reduceFunc),
            Some(reduceByKey),
            numPartitions = defaultN,
            resultPartitioning = Some(Partitioning(byKey = true, numPartitions = defaultN)),
          )

      case sortBy: Plan.SortByOp[_, _] =>
        val sourceStageId = buildStagesRecursive(ctx, sortBy.source, stageMap, dependencies)
        val n = SparkletConf.get.defaultShufflePartitions
        createShuffleStage(
          ctx,
          sourceStageId,
          "sortBy",
          stageMap,
          dependencies,
          Some(sortBy.keyFunc),
          Some(sortBy),
          numPartitions = n,
          resultPartitioning = None,
        )

      case pby: Plan.PartitionByOp[_, _] =>
        val sourceStageId = buildStagesRecursive(ctx, pby.source, stageMap, dependencies)
        createShuffleStage(
          ctx,
          sourceStageId,
          "partitionBy",
          stageMap,
          dependencies,
          None,
          Some(pby),
          numPartitions = pby.numPartitions,
          resultPartitioning = Some(Partitioning(byKey = true, numPartitions = pby.numPartitions)),
        )

      case rep: Plan.RepartitionOp[_] =>
        val sourceStageId = buildStagesRecursive(ctx, rep.source, stageMap, dependencies)
        createShuffleStage(
          ctx,
          sourceStageId,
          "repartition",
          stageMap,
          dependencies,
          None,
          Some(rep),
          numPartitions = rep.numPartitions,
          resultPartitioning = Some(Partitioning(byKey = false, numPartitions = rep.numPartitions)),
        )

      case coal: Plan.CoalesceOp[_] =>
        val sourceStageId = buildStagesRecursive(ctx, coal.source, stageMap, dependencies)
        createShuffleStage(
          ctx,
          sourceStageId,
          "coalesce",
          stageMap,
          dependencies,
          None,
          Some(coal),
          numPartitions = coal.numPartitions,
          resultPartitioning =
            Some(Partitioning(byKey = false, numPartitions = coal.numPartitions)),
        )

      case joinOp: Plan.JoinOp[_, _, _] =>
        val leftStageId = buildStagesRecursive(ctx, joinOp.left, stageMap, dependencies)
        val rightStageId = buildStagesRecursive(ctx, joinOp.right, stageMap, dependencies)

        // Join requires both inputs to be shuffled - create shuffle stage that reads from both
        val joinStageId = ctx.freshId()
        val joinStage =
          Stage.SingleOpStage[Any, Any](identity) // Placeholder for actual join logic

        // The join stage reads from the shuffle outputs of both left and right producing stages.
        // Record explicit upstream stage IDs with side tags to avoid heuristic lookups later.
        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val shuffleInputSources = Seq(
          TaggedShuffleFrom(leftStageId, Side.Left, numPartitions),
          TaggedShuffleFrom(rightStageId, Side.Right, numPartitions),
        )

        stageMap(joinStageId) = StageInfo(
          id = joinStageId,
          stage = joinStage,
          inputSources = shuffleInputSources,
          isShuffleStage = true,
          shuffleOperation = Some(joinOp), // Store the join operation for execution
          outputPartitioning = Some(Partitioning(byKey = true, numPartitions = numPartitions)),
        )

        dependencies.getOrElseUpdate(joinStageId, mutable.Set.empty) += leftStageId
        dependencies.getOrElseUpdate(joinStageId, mutable.Set.empty) += rightStageId
        joinStageId

      case cogroupOp: Plan.CoGroupOp[_, _, _] =>
        val leftStageId = buildStagesRecursive(ctx, cogroupOp.left, stageMap, dependencies)
        val rightStageId = buildStagesRecursive(ctx, cogroupOp.right, stageMap, dependencies)

        // CoGroup requires both inputs to be shuffled - create shuffle stage that reads from both
        val cogroupStageId = ctx.freshId()
        val cogroupStage =
          Stage.SingleOpStage[Any, Any](identity) // Placeholder for actual cogroup logic

        /* The cogroup stage reads from the shuffle outputs of both left and right producing
         * stages. */
        // Record explicit upstream stage IDs with side tags to avoid heuristic lookups later.
        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val shuffleInputSources = Seq(
          TaggedShuffleFrom(leftStageId, Side.Left, numPartitions),
          TaggedShuffleFrom(rightStageId, Side.Right, numPartitions),
        )

        stageMap(cogroupStageId) = StageInfo(
          id = cogroupStageId,
          stage = cogroupStage,
          inputSources = shuffleInputSources,
          isShuffleStage = true,
          shuffleOperation = Some(cogroupOp), // Store the cogroup operation for execution
          outputPartitioning = Some(Partitioning(byKey = true, numPartitions = numPartitions)),
        )

        dependencies.getOrElseUpdate(cogroupStageId, mutable.Set.empty) += leftStageId
        dependencies.getOrElseUpdate(cogroupStageId, mutable.Set.empty) += rightStageId
        cogroupStageId
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
   * Creates a new shuffle stage that depends on the source stage.
   *
   * TODO: Add actual shuffle logic, which will use the args operationType and operation.
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
    val shuffleStageId = ctx.freshId()

    // Create a placeholder stage for shuffle operations
    val shuffleStage =
      Stage.SingleOpStage[Any, Any](identity) // Will be replaced with actual shuffle logic

    /* Set up shuffle input source - the shuffle stage reads from the shuffle data produced by the
     * source stage */
    val shuffleInputSources = Seq(ShuffleFrom(sourceStageId, numPartitions))

    stageMap(shuffleStageId) = StageInfo(
      id = shuffleStageId,
      stage = shuffleStage,
      inputSources = shuffleInputSources,
      isShuffleStage = true,
      shuffleOperation = shuffleOperation,
      outputPartitioning = resultPartitioning,
    )

    dependencies.getOrElseUpdate(shuffleStageId, mutable.Set.empty) += sourceStageId
    shuffleStageId
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
