package com.ewoodbury.sparklet.execution

import scala.collection.mutable

import com.ewoodbury.sparklet.core.{Partition, Plan, ShuffleId, SparkletConf, StageId}

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

  /**
   * Information about a stage in the execution graph.
   */
  case class StageInfo(
      id: StageId,
      stage: Stage[_, _],
      inputSources: Seq[InputSource], // What this stage reads from
      isShuffleStage: Boolean,
      shuffleId: Option[ShuffleId], // For stages that produce shuffle output
      shuffleOperation: Option[Plan[_]], // The original Plan operation for shuffle stages
  )

  /**
   * Represents where a stage gets its input data from.
   */
  sealed trait InputSource
  case class SourceInput(partitions: Seq[Partition[_]]) extends InputSource
  case class ShuffleInput(shuffleId: ShuffleId, numPartitions: Int) extends InputSource

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
          shuffleId = None,
          shuffleOperation = None,
        )
        stageId

      // Narrow transformations - can be chained together
      case Plan.MapOp(sourcePlan, f) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendStageOrCreateNew(
          ctx,
          sourceStageId,
          Stage.map(f).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
        )

      case Plan.FilterOp(sourcePlan, p) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendStageOrCreateNew(
          ctx,
          sourceStageId,
          Stage.filter(p).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
        )

      case Plan.FlatMapOp(sourcePlan, f) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendStageOrCreateNew(
          ctx,
          sourceStageId,
          Stage.flatMap(f).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
        )

      case Plan.DistinctOp(sourcePlan) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendStageOrCreateNew(
          ctx,
          sourceStageId,
          Stage.distinct.asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
        )

      case Plan.KeysOp(sourcePlan) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendStageOrCreateNew(
          ctx,
          sourceStageId,
          Stage.keys.asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
        )

      case Plan.ValuesOp(sourcePlan) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendStageOrCreateNew(
          ctx,
          sourceStageId,
          Stage.values.asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
        )

      case Plan.MapValuesOp(sourcePlan, f) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendStageOrCreateNew(
          ctx,
          sourceStageId,
          Stage.mapValues(f).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
        )

      case Plan.FilterKeysOp(sourcePlan, p) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendStageOrCreateNew(
          ctx,
          sourceStageId,
          Stage.filterKeys(p).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
        )

      case Plan.FilterValuesOp(sourcePlan, p) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendStageOrCreateNew(
          ctx,
          sourceStageId,
          Stage.filterValues(p).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
        )

      case Plan.FlatMapValuesOp(sourcePlan, f) =>
        val sourceStageId = buildStagesRecursive(ctx, sourcePlan, stageMap, dependencies)
        extendStageOrCreateNew(
          ctx,
          sourceStageId,
          Stage.flatMapValues(f).asInstanceOf[Stage[Any, Any]],
          stageMap,
          dependencies,
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
          shuffleId = None,
          shuffleOperation = None,
        )

        dependencies.getOrElseUpdate(unionStageId, mutable.Set.empty) += leftStageId
        dependencies.getOrElseUpdate(unionStageId, mutable.Set.empty) += rightStageId
        unionStageId

      // --- Wide Transformations (create shuffle boundaries) ---
      case groupByKey: Plan.GroupByKeyOp[_, _] =>
        val sourceStageId = buildStagesRecursive(ctx, groupByKey.source, stageMap, dependencies)
        createShuffleStage(
          ctx,
          sourceStageId,
          "groupByKey",
          stageMap,
          dependencies,
          None,
          Some(groupByKey),
        )

      case reduceByKey: Plan.ReduceByKeyOp[_, _] =>
        val sourceStageId = buildStagesRecursive(ctx, reduceByKey.source, stageMap, dependencies)
        createShuffleStage(
          ctx,
          sourceStageId,
          "reduceByKey",
          stageMap,
          dependencies,
          Some(reduceByKey.reduceFunc),
          Some(reduceByKey),
        )

      case sortBy: Plan.SortByOp[_, _] =>
        val sourceStageId = buildStagesRecursive(ctx, sortBy.source, stageMap, dependencies)
        createShuffleStage(
          ctx,
          sourceStageId,
          "sortBy",
          stageMap,
          dependencies,
          Some(sortBy.keyFunc),
          Some(sortBy),
        )

      case joinOp: Plan.JoinOp[_, _, _] =>
        val leftStageId = buildStagesRecursive(ctx, joinOp.left, stageMap, dependencies)
        val rightStageId = buildStagesRecursive(ctx, joinOp.right, stageMap, dependencies)

        // Join requires both inputs to be shuffled - create shuffle stage that reads from both
        val joinStageId = ctx.freshId()
        val joinStage =
          Stage.SingleOpStage[Any, Any](identity) // Placeholder for actual join logic

        // The join stage reads from shuffle outputs of both left and right stages
        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val shuffleInputSources = Seq(
          ShuffleInput(
            ShuffleId.fromStageId(leftStageId),
            numPartitions,
          ), // Read from left stage shuffle output
          ShuffleInput(
            ShuffleId.fromStageId(rightStageId),
            numPartitions,
          ), // Read from right stage shuffle output
        )

        stageMap(joinStageId) = StageInfo(
          id = joinStageId,
          stage = joinStage,
          inputSources = shuffleInputSources,
          isShuffleStage = true,
          shuffleId = Some(ShuffleId.fromStageId(joinStageId)), // Use join stage ID as shuffle ID
          shuffleOperation = Some(joinOp), // Store the join operation for execution
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

        // The cogroup stage reads from shuffle outputs of both left and right stages
        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val shuffleInputSources = Seq(
          ShuffleInput(
            ShuffleId.fromStageId(leftStageId),
            numPartitions,
          ), // Read from left stage shuffle output
          ShuffleInput(
            ShuffleId.fromStageId(rightStageId),
            numPartitions,
          ), // Read from right stage shuffle output
        )

        stageMap(cogroupStageId) = StageInfo(
          id = cogroupStageId,
          stage = cogroupStage,
          inputSources = shuffleInputSources,
          isShuffleStage = true,
          shuffleId =
            Some(ShuffleId.fromStageId(cogroupStageId)), // Use cogroup stage ID as shuffle ID
          shuffleOperation = Some(cogroupOp), // Store the cogroup operation for execution
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

      // Set up input sources to read from the shuffle output of the source stage
      val shuffleInputSources = sourceStage.shuffleId match {
        case Some(shuffleId) =>
          Seq(ShuffleInput(shuffleId, SparkletConf.get.defaultShufflePartitions))
        case None =>
          throw new IllegalStateException(
            s"Shuffle stage ${sourceStageId.toInt} has no shuffle ID",
          )
      }

      stageMap(newStageId) = StageInfo(
        id = newStageId,
        stage = newOperation,
        inputSources = shuffleInputSources,
        isShuffleStage = false,
        shuffleId = None,
        shuffleOperation = None,
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
  ): StageId = {
    val shuffleStageId = ctx.freshId()
    val shuffleId =
      ShuffleId.fromStageId(sourceStageId) // Use source stage ID as shuffle ID for consistency

    // Create a placeholder stage for shuffle operations
    val shuffleStage =
      Stage.SingleOpStage[Any, Any](identity) // Will be replaced with actual shuffle logic

    /* Set up shuffle input source - the shuffle stage reads from the shuffle data produced by the
     * source stage */
    val numPartitions = SparkletConf.get.defaultShufflePartitions
    val shuffleInputSources = Seq(ShuffleInput(shuffleId, numPartitions))

    stageMap(shuffleStageId) = StageInfo(
      id = shuffleStageId,
      stage = shuffleStage,
      inputSources = shuffleInputSources,
      isShuffleStage = true,
      shuffleId = Some(shuffleId),
      shuffleOperation = shuffleOperation,
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
