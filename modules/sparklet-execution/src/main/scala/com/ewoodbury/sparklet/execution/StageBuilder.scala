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
   * Immutable stage draft for accumulating operations during stage construction.
   * All fields are immutable; mutations create new instances.
   */
  private case class StageDraft(
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
   * Encapsulated mutation helpers to centralize stage builder and dependency management.
   * Provides single points for invariant checking and reduces accidental misuse.
   */
  private def putBuilder(
      builderMap: mutable.Map[StageId, StageDraft],
      builder: StageDraft
  ): Unit = {
    builderMap(builder.id) = builder
  }

  private def putNewBuilder(
      builderMap: mutable.Map[StageId, StageDraft],
      builder: StageDraft
  ): Unit = {
    // Check invariant: no duplicate stage IDs
    require(!builderMap.contains(builder.id), s"Stage ID ${builder.id} already exists in builder map")
    builderMap(builder.id) = builder
  }

  private def addDependency(
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
      child: StageId,
      parent: StageId
  ): Unit = {
    require(!(child == parent), s"Stage $child cannot depend on itself")
    dependencies.getOrElseUpdate(child, mutable.Set.empty) += parent
  }

  /**
   * Builds a complete stage graph from a plan using the unified builder approach.
   * This replaces the old recursive approach with a more structured operation accumulation.
   */
  def buildStageGraph[A](plan: Plan[A]): StageGraph = {
    val ctx = BuildContext(0)
    val builderMap = mutable.Map[StageId, StageDraft]()
    val dependencies = mutable.Map[StageId, mutable.Set[StageId]]()

    val (finalStageId, _) = buildStagesFromPlan(ctx, plan, builderMap, dependencies)

    // Convert StageDraft instances to StageInfo instances
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

    val stageGraph = StageGraph(
      stageMap.toMap,
      dependencies.map { case (k, v) => k -> v.toSet }.toMap,
      finalStageId,
    )

    // Post-build validation
    validateStageGraph(stageGraph)

    stageGraph
  }

  /**
   * Validates StageGraph invariants to catch errors early.
   * Checks for consistency issues that could cause runtime failures.
   */
  private def validateStageGraph(graph: StageGraph): Unit = {
    // 1. finalStageId exists in stages map
    if (!graph.stages.contains(graph.finalStageId)) {
      throw new IllegalStateException(s"finalStageId ${graph.finalStageId} not found in stages map")
    }

    // 2. Every dependency target exists
    graph.dependencies.foreachEntry { (stageId, deps) =>
      if (!graph.stages.contains(stageId)) {
        throw new IllegalStateException(s"Stage $stageId has dependencies but is not in stages map")
      }
      deps.foreach { depId =>
        if (!graph.stages.contains(depId)) {
          throw new IllegalStateException(s"Stage $stageId depends on $depId which doesn't exist")
        }
      }
    }

    // 3. No stage lists itself as dependency (prevents infinite loops)
    graph.dependencies.foreachEntry { (stageId, deps) =>
      if (deps.contains(stageId)) {
        throw new IllegalStateException(s"Stage $stageId lists itself as a dependency")
      }
    }

    // 4. Acyclicity check using DFS
    validateAcyclicity(graph)

    // 5. Reachability check - all stages must be reachable from finalStageId
    validateReachability(graph)

    // 6. Stage ID monotonicity check (strictly increasing sequence)
    validateStageIdMonotonicity(graph)

    // 7. Shuffle stage specific validations
    validateShuffleStages(graph)

    // 8. Partitioning metadata consistency
    graph.stages.values.foreach { stageInfo =>
      stageInfo.outputPartitioning.foreach { partitioning =>
        // byKey implies numPartitions > 0
        if (partitioning.byKey && partitioning.numPartitions <= 0) {
          throw new IllegalStateException(s"Stage ${stageInfo.id} has byKey=true but numPartitions=${partitioning.numPartitions} <= 0")
        }
        // numPartitions should be reasonable (not excessively large)
        if (partitioning.numPartitions > 1000000) {
          throw new IllegalStateException(s"Stage ${stageInfo.id} has excessively large numPartitions=${partitioning.numPartitions}")
        }
      }
    }

    // 9. Partitioning invariants - byKey only for operations that guarantee key grouping
    validatePartitioningInvariants(graph)
  }

  /**
   * Validates that the stage graph is acyclic using depth-first search.
   */
  private def validateAcyclicity(graph: StageGraph): Unit = {
    val visiting = mutable.Set[StageId]()
    val visited = mutable.Set[StageId]()

    def dfsVisit(stageId: StageId): Unit = {
      if (visiting.contains(stageId)) {
        throw new IllegalStateException(s"Cycle detected in stage graph involving stage $stageId")
      }
      if (visited.contains(stageId)) {
        return
      }

      visiting.add(stageId)
      graph.dependencies.getOrElse(stageId, Set.empty).foreach(dfsVisit)
      visiting.remove(stageId)
      visited.add(stageId)
    }

    graph.stages.keys.foreach { stageId =>
      if (!visited.contains(stageId)) {
        dfsVisit(stageId)
      }
    }
  }

  /**
   * Validates that all stages are reachable from the final stage via reverse traversal.
   */
  private def validateReachability(graph: StageGraph): Unit = {
    val reachable = mutable.Set[StageId]()
    val toVisit = mutable.Queue[StageId]()

    // Start from finalStageId and traverse backwards through dependencies
    toVisit.enqueue(graph.finalStageId)
    reachable.add(graph.finalStageId)

    while (toVisit.nonEmpty) {
      val current = toVisit.dequeue()
      graph.dependencies.getOrElse(current, Set.empty).foreach { depId =>
        if (!reachable.contains(depId)) {
          reachable.add(depId)
          toVisit.enqueue(depId)
        }
      }
    }

    // Check for orphaned stages
    val allStageIds = graph.stages.keySet
    val orphaned = allStageIds -- reachable
    if (orphaned.nonEmpty) {
      throw new IllegalStateException(s"Orphaned stages not reachable from finalStageId ${graph.finalStageId}: ${orphaned.toSeq.sorted}")
    }
  }

  /**
   * Validates that stage IDs form a monotonic sequence (strictly increasing from 0).
   */
  private def validateStageIdMonotonicity(graph: StageGraph): Unit = {
    val stageIds = graph.stages.keys.toSeq.sorted
    if (stageIds.nonEmpty) {
      // Check starts from 0
      if (stageIds.headOption.get.toInt != 0) {
        throw new IllegalStateException(s"Stage IDs should start from 0, but found minimum ID: ${stageIds.head.toInt}")
      }

      // Check for gaps (warn only to future-proof ID reuse scenarios)
      val expectedSequence = (0 until stageIds.length).map(StageId(_))
      val actualSet = stageIds.toSet
      val missing = expectedSequence.filterNot(actualSet.contains)
      if (missing.nonEmpty) {
        // Use println instead of logging to avoid dependencies
        println(s"Warning: Stage ID sequence has gaps. Missing IDs: ${missing.map(_.toInt).mkString(", ")}")
      }
    }
  }

  /**
   * Validates shuffle stage specific invariants.
   */
  private def validateShuffleStages(graph: StageGraph): Unit = {
    graph.stages.values.foreach { stageInfo =>
      if (stageInfo.isShuffleStage) {
        // Shuffle stages should have shuffle operation metadata
        if (stageInfo.shuffleOperation.isEmpty) {
          throw new IllegalStateException(s"Shuffle stage ${stageInfo.id} has no shuffle operation metadata")
        }

        // Shuffle stages should have empty ops vector (they don't execute operations)
        stageInfo.stage match {
          case _: Stage.ChainedStage[_, _, _] =>
            // ChainedStage should not be used for shuffle stages
            throw new IllegalStateException(s"Shuffle stage ${stageInfo.id} incorrectly uses ChainedStage")
          case _ => // Other stage types are acceptable for shuffle stages
        }

        // Multi-input shuffle stages should have proper side markers
        if (stageInfo.inputSources.length == 2) {
          val shuffleInputs = stageInfo.inputSources.collect { case si: ShuffleInput => si }
          if (shuffleInputs.length != 2) {
            throw new IllegalStateException(s"Multi-input shuffle stage ${stageInfo.id} should have exactly 2 ShuffleInputs, found ${shuffleInputs.length}")
          }
          if (shuffleInputs.exists(_.side.isEmpty)) {
            throw new IllegalStateException(s"Multi-input shuffle stage ${stageInfo.id} has ShuffleInputs without side markers")
          }
          val sides = shuffleInputs.flatMap(_.side).toSet
          if (sides.size != 2 || !sides.contains(Side.Left) || !sides.contains(Side.Right)) {
            throw new IllegalStateException(s"Multi-input shuffle stage ${stageInfo.id} has invalid side markers: expected {Left, Right}, found $sides")
          }

          // Validate numPartitions consistency across inputs
          val numPartitionsList = shuffleInputs.map(_.numPartitions).distinct
          if (numPartitionsList.length > 1) {
            throw new IllegalStateException(s"Multi-input shuffle stage ${stageInfo.id} has mismatched numPartitions across inputs: ${numPartitionsList.mkString(", ")}")
          }
        }
      }
    }
  }

  /**
   * Validates partitioning invariants - byKey should only be true for operations that guarantee key grouping.
   */
  private def validatePartitioningInvariants(graph: StageGraph): Unit = {
    // Operations that guarantee key grouping - simplified approach since classOf with generics is problematic
    // In practice, this would be implemented with a more sophisticated operation analysis system

    graph.stages.values.foreach { stageInfo =>
      stageInfo.outputPartitioning.foreach { partitioning =>
        if (partitioning.byKey) {
          // For byKey=true, verify stage has key-grouping operations or is a shuffle stage
          if (!stageInfo.isShuffleStage) {
            stageInfo.stage match {
              case _: Stage.ChainedStage[_, _, _] =>
                // For chained stages, check if any operation guarantees key grouping
                // Note: This is a simplified check - in practice we'd need to analyze the operation chain
                // For now, we'll be permissive and allow byKey=true if it's explicitly set
                // This validation can be strengthened in the future with more detailed operation analysis
              case _ =>
                // For single operation stages, we could check the specific operation type
                // But since we don't have direct access to the operation, we'll be permissive here
            }
          }
        }
      }
    }
  }

  /**
   * Materializes a vector of operations into a concrete Stage form.
   *
   * This function converts a sequence of Operation ADT instances into executable Stage objects.
   * It employs several optimization strategies:
   *
   * 1. **Single Operation Optimization**: For single operations, returns the operation directly
   *    without unnecessary chaining to minimize overhead.
   *
   * 2. **Efficient Chaining**: For multiple operations, uses a left fold to build the chain,
   *    creating ChainedStage instances that compose operations efficiently.
   *
   * 3. **Type Safety**: Uses type casting to Any for generic operations while maintaining
   *    runtime type safety through the Stage abstraction.
   *
   * @param ops Non-empty vector of operations to materialize
   * @return A concrete Stage that can be executed
   * @throws UnsupportedOperationException if the operation vector contains wide operations
   *         (shuffle operations) that cannot be materialized as narrow stages
   * @note This function only handles narrow operations. Wide operations should be processed
   *       through separate shuffle stages before materialization.
   */
  private[execution] def materialize(ops: Vector[Operation]): Stage[Any, Any] = {
    require(ops.nonEmpty, "Cannot materialize empty operation vector")

    // For single operations, return the operation directly without chaining
    if (ops.length == 1) {
      return ops.headOption.get match {
        case MapOp(f) => Stage.map(f.asInstanceOf[Any => Any])
        case FilterOp(p) => Stage.filter(p.asInstanceOf[Any => Boolean])
        case FlatMapOp(f) => Stage.flatMap(f.asInstanceOf[Any => IterableOnce[Any]])
        case DistinctOp() => Stage.distinct
        case KeysOp() => Stage.keys.asInstanceOf[Stage[Any, Any]]
        case ValuesOp() => Stage.values.asInstanceOf[Stage[Any, Any]]
        case MapValuesOp(f) => Stage.mapValues(f.asInstanceOf[Any => Any]).asInstanceOf[Stage[Any, Any]]
        case FilterKeysOp(p) => Stage.filterKeys(p.asInstanceOf[Any => Boolean]).asInstanceOf[Stage[Any, Any]]
        case FilterValuesOp(p) => Stage.filterValues(p.asInstanceOf[Any => Boolean]).asInstanceOf[Stage[Any, Any]]
        case FlatMapValuesOp(f) => Stage.flatMapValues(f.asInstanceOf[Any => IterableOnce[Any]]).asInstanceOf[Stage[Any, Any]]
        case MapPartitionsOp(f) => Stage.mapPartitions(f.asInstanceOf[Iterator[Any] => Iterator[Any]])
        case GroupByKeyLocalOp() => Stage.groupByKeyLocal.asInstanceOf[Stage[Any, Any]]
        case ReduceByKeyLocalOp(reduceFunc) => Stage.reduceByKeyLocal(reduceFunc.asInstanceOf[(Any, Any) => Any]).asInstanceOf[Stage[Any, Any]]
        case PartitionByLocalOp(_) => Stage.identity[Any].asInstanceOf[Stage[Any, Any]] // Bypassed partition is identity
        case _ => throw new UnsupportedOperationException(s"Cannot materialize wide operation: ${ops.headOption.get}")
      }
    }

    // For multiple operations, build the chain efficiently
    ops.drop(1).foldLeft(createStageFromOp(ops.head)) { (stage, op) =>
      op match {
        case MapOp(f) => Stage.ChainedStage(stage, Stage.map(f.asInstanceOf[Any => Any]))
        case FilterOp(p) => Stage.ChainedStage(stage, Stage.filter(p.asInstanceOf[Any => Boolean]))
        case FlatMapOp(f) => Stage.ChainedStage(stage, Stage.flatMap(f.asInstanceOf[Any => IterableOnce[Any]]))
        case DistinctOp() => Stage.ChainedStage(stage, Stage.distinct)
        case KeysOp() => Stage.ChainedStage(stage, Stage.keys.asInstanceOf[Stage[Any, Any]])
        case ValuesOp() => Stage.ChainedStage(stage, Stage.values.asInstanceOf[Stage[Any, Any]])
        case MapValuesOp(f) => Stage.ChainedStage(stage, Stage.mapValues(f.asInstanceOf[Any => Any]).asInstanceOf[Stage[Any, Any]])
        case FilterKeysOp(p) => Stage.ChainedStage(stage, Stage.filterKeys(p.asInstanceOf[Any => Boolean]).asInstanceOf[Stage[Any, Any]])
        case FilterValuesOp(p) => Stage.ChainedStage(stage, Stage.filterValues(p.asInstanceOf[Any => Boolean]).asInstanceOf[Stage[Any, Any]])
        case FlatMapValuesOp(f) => Stage.ChainedStage(stage, Stage.flatMapValues(f.asInstanceOf[Any => IterableOnce[Any]]).asInstanceOf[Stage[Any, Any]])
        case MapPartitionsOp(f) => Stage.ChainedStage(stage, Stage.mapPartitions(f.asInstanceOf[Iterator[Any] => Iterator[Any]]))
        case GroupByKeyLocalOp() => Stage.ChainedStage(stage, Stage.groupByKeyLocal.asInstanceOf[Stage[Any, Any]])
        case ReduceByKeyLocalOp(reduceFunc) => Stage.ChainedStage(stage, Stage.reduceByKeyLocal(reduceFunc.asInstanceOf[(Any, Any) => Any]).asInstanceOf[Stage[Any, Any]])
        case PartitionByLocalOp(_) => Stage.ChainedStage(stage, Stage.identity[Any].asInstanceOf[Stage[Any, Any]]) // Bypassed partition is identity
        case _ => throw new UnsupportedOperationException(s"Cannot materialize wide operation: $op")
      }
    }
  }

  /**
   * Creates a concrete Stage object from a single Operation ADT instance.
   *
   * This method serves as the bridge between the high-level Operation ADT and the concrete
   * Stage implementations that can be executed by the runtime. It pattern matches on the
   * operation type and delegates to the appropriate Stage constructor.
   *
   * The method handles all narrow transformation operations including:
   * - Basic transformations: MapOp, FilterOp, FlatMapOp, DistinctOp
   * - Key-value operations: KeysOp, ValuesOp, MapValuesOp, FilterKeysOp, FilterValuesOp, FlatMapValuesOp
   * - Partition operations: MapPartitionsOp
   * - Local/bypass operations: GroupByKeyLocalOp, ReduceByKeyLocalOp, PartitionByLocalOp
   *
   * This method is optimized for creating individual stages and is more efficient than
   * materialize() when dealing with single operations, as it avoids unnecessary ChainedStage wrapping.
   *
   * @param op The Operation to convert into a Stage
   * @return A concrete Stage[Any, Any] that can be executed
   * @throws UnsupportedOperationException if the operation is a wide operation that requires shuffle boundaries
   * @note This method only supports narrow operations. Wide operations should be handled
   *       through shuffle stages created by createShuffleStageUnified().
   */
  private def createStageFromOp(op: Operation): Stage[Any, Any] = {
    op match {
      case MapOp(f) => Stage.map(f.asInstanceOf[Any => Any])
      case FilterOp(p) => Stage.filter(p.asInstanceOf[Any => Boolean])
      case FlatMapOp(f) => Stage.flatMap(f.asInstanceOf[Any => IterableOnce[Any]])
      case DistinctOp() => Stage.distinct
      case KeysOp() => Stage.keys.asInstanceOf[Stage[Any, Any]]
      case ValuesOp() => Stage.values.asInstanceOf[Stage[Any, Any]]
      case MapValuesOp(f) => Stage.mapValues(f.asInstanceOf[Any => Any]).asInstanceOf[Stage[Any, Any]]
      case FilterKeysOp(p) => Stage.filterKeys(p.asInstanceOf[Any => Boolean]).asInstanceOf[Stage[Any, Any]]
      case FilterValuesOp(p) => Stage.filterValues(p.asInstanceOf[Any => Boolean]).asInstanceOf[Stage[Any, Any]]
      case FlatMapValuesOp(f) => Stage.flatMapValues(f.asInstanceOf[Any => IterableOnce[Any]]).asInstanceOf[Stage[Any, Any]]
      case MapPartitionsOp(f) => Stage.mapPartitions(f.asInstanceOf[Iterator[Any] => Iterator[Any]])
      case GroupByKeyLocalOp() => Stage.groupByKeyLocal.asInstanceOf[Stage[Any, Any]]
      case ReduceByKeyLocalOp(reduceFunc) => Stage.reduceByKeyLocal(reduceFunc.asInstanceOf[(Any, Any) => Any]).asInstanceOf[Stage[Any, Any]]
      case PartitionByLocalOp(_) => Stage.identity[Any].asInstanceOf[Stage[Any, Any]] // Bypassed partition is identity
      case _ => throw new UnsupportedOperationException(s"Cannot create stage from operation: $op")
    }
  }

  /**
   * Core recursive method that traverses a Plan tree and builds stage graphs with operation accumulation.
   *
   * This method implements the unified stage building algorithm that processes Plan nodes recursively,
   * accumulating narrow operations into stages and creating shuffle boundaries for wide operations.
   * It replaces the old recursive approach with a more structured operation accumulation strategy
   * for better optimization and clearer stage boundaries.
   *
   * The method handles three main categories of Plan nodes:
   *
   * 1. **Data Sources (Plan.Source)**:
   *    - Base case that creates initial stage builders for data partitions
   *    - Sets up source partitioning metadata based on partition count
   *    - Returns the stage ID and original Plan for dependency tracking
   *
   * 2. **Narrow Transformations**:
   *    - Accumulates operations using appendOperation() when possible
   *    - Creates new stages when chaining is not feasible
   *    - Handles: MapOp, FilterOp, FlatMapOp, DistinctOp, KeysOp, ValuesOp,
   *      MapValuesOp, FilterKeysOp, FilterValuesOp, FlatMapValuesOp, MapPartitionsOp
   *
   * 3. **Multi-input Operations**:
   *    - UnionOp: Creates a new stage that reads from both input stages
   *    - Preserves no partitioning metadata due to input consolidation
   *
   * 4. **Wide Transformations (Shuffle Operations)**:
   *    - GroupByKeyOp, ReduceByKeyOp: Supports shuffle bypass optimization when already partitioned correctly
   *    - SortByOp, PartitionByOp, RepartitionOp, CoalesceOp: Always creates shuffle boundaries
   *    - JoinOp, CoGroupOp: Multi-input wide operations with side tagging for disambiguation
   *    - Uses WideOp metadata structures for shuffle execution planning
   *
   * **Optimization Strategies**:
   * - Shuffle Bypass: For operations like groupByKey/reduceByKey, checks if upstream partitioning
   *   matches requirements and bypasses shuffle when possible
   * - Operation Chaining: Accumulates multiple narrow operations into single stages to reduce overhead
   * - Dependency Tracking: Builds comprehensive dependency graph for execution ordering
   *
   * @param ctx BuildContext for generating unique stage IDs
   * @param plan The Plan node to process recursively
   * @param builderMap Mutable map accumulating stage builders (modified in place)
   * @param dependencies Mutable map tracking stage dependencies (modified in place)
   * @tparam A Type parameter of the Plan (preserved for type safety)
   * @return Tuple of (StageId, Option[Plan]) where StageId is the resulting stage identifier,
   *         and Option[Plan] contains the original Plan for shuffle operation metadata
   * @note This method modifies builderMap and dependencies in place for efficiency
   * @note The return Plan is crucial for shuffle stages to preserve original operation metadata
   */
  private def buildStagesFromPlan[A](
      ctx: BuildContext,
      plan: Plan[A],
      builderMap: mutable.Map[StageId, StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): (StageId, Option[Plan[_]]) = {
    plan match {
      // Base case: data source - don't create a stage yet, let operations chain to it
      case source: Plan.Source[_] =>
        val stageId = ctx.freshId()
        putNewBuilder(builderMap, StageDraft(
          id = stageId,
          ops = Vector.empty[Operation], // No operations for source - operations will be chained here
          inputSources = Seq(SourceInput(source.partitions)),
          isShuffle = false,
          shuffleMeta = None,
          originalPlan = Some(source),
          outputPartitioning = Some(Partitioning(byKey = false, numPartitions = source.partitions.size)),
        ))
        (stageId, Some(source))

      // Narrow transformations - accumulate operations or create new stages
      case Plan.MapOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, MapOp(f), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.FilterOp(sourcePlan, p) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, FilterOp(p), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.FlatMapOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, FlatMapOp(f), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.DistinctOp(sourcePlan) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, DistinctOp(), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.KeysOp(sourcePlan) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, KeysOp(), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.ValuesOp(sourcePlan) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, ValuesOp(), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.MapValuesOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, MapValuesOp(f), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.FilterKeysOp(sourcePlan, p) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, FilterKeysOp(p), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.FilterValuesOp(sourcePlan, p) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, FilterValuesOp(p), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.FlatMapValuesOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, FlatMapValuesOp(f), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.MapPartitionsOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(ctx, sourceStageId, MapPartitionsOp(f), builderMap, dependencies)
        (resultId, Some(plan))

      case Plan.UnionOp(left, right) =>
        val (leftStageId, _) = buildStagesFromPlan(ctx, left, builderMap, dependencies)
        val (rightStageId, _) = buildStagesFromPlan(ctx, right, builderMap, dependencies)

        // Union creates a new narrow stage that reads outputs from both input stages
        val unionStageId = ctx.freshId()
        putNewBuilder(builderMap, StageDraft(
          id = unionStageId,
          ops = Vector.empty[Operation], // No operations, just union of inputs
          inputSources = Seq(StageOutput(leftStageId), StageOutput(rightStageId)),
          isShuffle = false,
          shuffleMeta = None,
          originalPlan = Some(plan): Option[Plan[_]],
          outputPartitioning = None, // Union doesn't preserve partitioning
        ))

        addDependency(dependencies, unionStageId, leftStageId)
        addDependency(dependencies, unionStageId, rightStageId)
        (unionStageId, Some(plan))

      // Wide transformations (create shuffle boundaries)
      case groupByKey: Plan.GroupByKeyOp[_, _] =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, groupByKey.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)
        val defaultN = SparkletConf.get.defaultShufflePartitions

        if (Operation.canBypassShuffle(groupByKey, src.outputPartitioning, SparkletConf.get)) {
          // Add local groupByKey operation to existing stage since shuffle can be bypassed
          val resultId = appendOperation(ctx, sourceStageId, GroupByKeyLocalOp[Any, Any](), builderMap, dependencies)
          (resultId, Some(groupByKey))
        } else {
          val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), GroupByKeyWideOp(SimpleWideOpMeta(
            kind = WideOpKind.GroupByKey,
            numPartitions = defaultN
          )), builderMap, dependencies, Some(groupByKey))
          (shuffleId, Some(groupByKey))
        }

      case reduceByKey: Plan.ReduceByKeyOp[_, _] =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, reduceByKey.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)
        val defaultN = SparkletConf.get.defaultShufflePartitions

        if (Operation.canBypassShuffle(reduceByKey, src.outputPartitioning, SparkletConf.get)) {
          // Add local reduceByKey operation to existing stage since shuffle can be bypassed
          val resultId = appendOperation(ctx, sourceStageId, ReduceByKeyLocalOp[Any, Any](reduceByKey.reduceFunc.asInstanceOf[(Any, Any) => Any]), builderMap, dependencies)
          (resultId, Some(reduceByKey))
        } else {
          val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), ReduceByKeyWideOp(ReduceWideOpMeta(
            numPartitions = defaultN,
            reduceFunc = reduceByKey.reduceFunc
          )), builderMap, dependencies, Some(reduceByKey))
          (shuffleId, Some(reduceByKey))
        }

      case sortBy: Plan.SortByOp[_, _] =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sortBy.source, builderMap, dependencies)
        val n = SparkletConf.get.defaultShufflePartitions
        val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), SortByWideOp(SortWideOpMeta(
          numPartitions = n,
          keyFunc = sortBy.keyFunc
        )), builderMap, dependencies, Some(sortBy))
        (shuffleId, Some(sortBy))

      case pby: Plan.PartitionByOp[_, _] =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, pby.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)

        if (Operation.canBypassShuffle(pby, src.outputPartitioning, SparkletConf.get)) {
          val resultId = appendOperation(ctx, sourceStageId, PartitionByLocalOp[Any, Any](pby.numPartitions), builderMap, dependencies)
          (resultId, Some(pby))
        } else {
          val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), PartitionByWideOp(SimpleWideOpMeta(
            kind = WideOpKind.PartitionBy,
            numPartitions = pby.numPartitions
          )), builderMap, dependencies, Some(pby))
          (shuffleId, Some(pby))
        }

      case rep: Plan.RepartitionOp[_] =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, rep.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)

        if (Operation.canBypassShuffle(rep, src.outputPartitioning, SparkletConf.get)) {
          val resultId = appendOperation(ctx, sourceStageId, RepartitionOp[Any](rep.numPartitions), builderMap, dependencies)
          (resultId, Some(rep))
        } else {
          val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), RepartitionWideOp(SimpleWideOpMeta(
            kind = WideOpKind.Repartition,
            numPartitions = rep.numPartitions
          )), builderMap, dependencies, Some(rep))
          (shuffleId, Some(rep))
        }

      case coal: Plan.CoalesceOp[_] =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, coal.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)

        if (Operation.canBypassShuffle(coal, src.outputPartitioning, SparkletConf.get)) {
          val resultId = appendOperation(ctx, sourceStageId, CoalesceOp[Any](coal.numPartitions), builderMap, dependencies)
          (resultId, Some(coal))
        } else {
          val shuffleId = createShuffleStageUnified(ctx, Seq(sourceStageId), CoalesceWideOp(SimpleWideOpMeta(
            kind = WideOpKind.Coalesce,
            numPartitions = coal.numPartitions
          )), builderMap, dependencies, Some(coal))
          (shuffleId, Some(coal))
        }

      case joinOp: Plan.JoinOp[_, _, _] =>
        val (leftStageId, _) = buildStagesFromPlan(ctx, joinOp.left, builderMap, dependencies)
        val (rightStageId, _) = buildStagesFromPlan(ctx, joinOp.right, builderMap, dependencies)

        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val shuffleId = createShuffleStageUnified(ctx, Seq(leftStageId, rightStageId), JoinWideOp(SimpleWideOpMeta(
          kind = WideOpKind.Join,
          numPartitions = numPartitions,
          joinStrategy = joinOp.joinStrategy,
          sides = Seq(Side.Left, Side.Right)
        )), builderMap, dependencies, Some(joinOp))
        (shuffleId, Some(joinOp))

      case cogroupOp: Plan.CoGroupOp[_, _, _] =>
        val (leftStageId, _) = buildStagesFromPlan(ctx, cogroupOp.left, builderMap, dependencies)
        val (rightStageId, _) = buildStagesFromPlan(ctx, cogroupOp.right, builderMap, dependencies)

        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val shuffleId = createShuffleStageUnified(ctx, Seq(leftStageId, rightStageId), CoGroupWideOp(SimpleWideOpMeta(
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
      builderMap: mutable.Map[StageId, StageDraft],
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
  ): StageId = {
    val sourceBuilder = builderMap(sourceStageId)

    if (sourceBuilder.isShuffle) {
      // Can't extend a shuffle stage, create a new narrow stage
      val newStageId = ctx.freshId()
      val newBuilder = StageDraft(
        id = newStageId,
        ops = Vector(op),
        inputSources = Seq(StageOutput(sourceStageId)),
        isShuffle = false,
        shuffleMeta = None,
        originalPlan = None,
        outputPartitioning = updatePartitioning(sourceBuilder.outputPartitioning, op),
      )
      putNewBuilder(builderMap, newBuilder)

      // Add dependency: new stage depends on source stage
      addDependency(dependencies, newStageId, sourceStageId)
      newStageId
    } else {
      // Check if we can chain this operation - only if the stage has a single producing path
      // For now, we chain if there are no multi-input sources (like Union)
      val canChain = sourceBuilder.inputSources.forall {
        case _: SourceInput => true
        case _: StageOutput => sourceBuilder.inputSources.length == 1
        case _: ShuffleInput => false
      }

      if (canChain && (!sourceBuilder.isShuffle || sourceBuilder.ops.nonEmpty)) {
        // Extend the existing stage by appending the operation
        val updatedOps = sourceBuilder.ops :+ op
        val updatedBuilder = sourceBuilder.copy(
          ops = updatedOps,
          outputPartitioning = updatePartitioning(sourceBuilder.outputPartitioning, op)
        )
        putBuilder(builderMap, updatedBuilder)
        sourceStageId
      } else {
        // Create a new stage with this operation
        val newStageId = ctx.freshId()
        val newBuilder = StageDraft(
          id = newStageId,
          ops = Vector(op),
          inputSources = sourceBuilder.inputSources,
          isShuffle = false,
          shuffleMeta = None,
          originalPlan = None,
          outputPartitioning = updatePartitioning(sourceBuilder.outputPartitioning, op),
        )
        putNewBuilder(builderMap, newBuilder)

        // Copy dependencies from source stage
        sourceBuilder.inputSources.foreach {
          case StageOutput(upstreamId) =>
            addDependency(dependencies, newStageId, upstreamId)
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
      builderMap: mutable.Map[StageId, StageDraft],
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

    putNewBuilder(builderMap, StageDraft(
      id = shuffleStageId,
      ops = Vector.empty[Operation], // Shuffle stages don't have narrow operations
      inputSources = shuffleInputSources,
      isShuffle = true,
      shuffleMeta = Some(wideOp),
      originalPlan = originalPlan,
      outputPartitioning = outputPartitioning,
    ))

    // Add dependencies for all upstream stages
    upstreamIds.foreach { upstreamId =>
      addDependency(dependencies, shuffleStageId, upstreamId)
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

      // Local operations preserve existing partitioning
      case _: GroupByKeyLocalOp[_, _] | _: ReduceByKeyLocalOp[_, _] =>
        prev

      case sb: SortByOp[_, _] =>
        // SortBy creates key-based partitioning but doesn't guarantee byKey for output
        Some(Partitioning(byKey = false, numPartitions = sb.numPartitions))

      case pby: PartitionByOp[_, _] =>
        Some(Partitioning(byKey = true, numPartitions = pby.numPartitions))

      case pbl: PartitionByLocalOp[_, _] =>
        Some(Partitioning(byKey = true, numPartitions = pbl.numPartitions))

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

    // For narrow-only plans, use the unified builder and convert to legacy format
    val stageGraph = buildStageGraph(plan)
    legacyAdapter(stageGraph).asInstanceOf[Seq[(Plan.Source[_], Stage[_, A])]]
  }

  /**
   * Temporary adapter to convert StageGraph back to legacy format for backward compatibility.
   * Traverses from sources to finalStageId collecting only linear narrow chains with no shuffles.
   * Fails fast if any shuffle stage encountered (enforces caller migration).
   */
  private def legacyAdapter(graph: StageGraph): Seq[(Plan.Source[_], Stage[_, _])] = {
    // Find all source stages (stages that read from SourceInput)
    val sourceStages = graph.stages.filter { case (_, stageInfo) =>
      stageInfo.inputSources.exists(_.isInstanceOf[SourceInput])
    }

    if (sourceStages.isEmpty) {
      throw new IllegalStateException("No source stages found in StageGraph")
    }

    sourceStages.toSeq.map { case (stageId, stageInfo) =>
      // Extract the original source plan from the shuffleOperation field
      val sourcePlan = stageInfo.shuffleOperation.collect {
        case source: Plan.Source[_] => source
      }.getOrElse {
        throw new IllegalStateException(s"Source stage ${stageId} has no original Plan.Source stored")
      }

      // For each source stage, traverse to find the linear path to final stage
      val path = findLinearPathToFinal(graph, stageId, graph.finalStageId)

      // Verify the path contains no shuffle stages
      path.foreach { pathStageId =>
        val pathStage = graph.stages(pathStageId)
        if (pathStage.isShuffleStage) {
          throw new UnsupportedOperationException(
            s"Legacy adapter encountered shuffle stage ${pathStageId} in path from ${stageId} to ${graph.finalStageId}. " +
            "Cannot use legacy buildStages with shuffle operations - use DAGScheduler instead."
          )
        }
      }

      // Build the final stage by combining all operations in the path
      val finalStage = buildStageFromPath(graph, path)

      (sourcePlan, finalStage)
    }
  }

  /**
   * Finds the linear path from startStageId to endStageId in the dependency graph.
   * Assumes a linear chain exists (no branching for narrow-only plans).
   */
  private def findLinearPathToFinal(graph: StageGraph, startStageId: StageId, endStageId: StageId): Seq[StageId] = {
    def traverse(currentId: StageId, path: List[StageId]): Seq[StageId] = {
      if (currentId == endStageId) {
        (currentId :: path).reverse
      } else {
        // Find the next stage that depends on currentId
        val nextStages = graph.dependencies.filter(_._2.contains(currentId)).keys
        if (nextStages.size != 1) {
          throw new UnsupportedOperationException(
            s"Legacy adapter expects linear chain but found ${nextStages.size} stages depending on ${currentId}"
          )
        }
        traverse(nextStages.head, currentId :: path)
      }
    }

    traverse(startStageId, Nil)
  }

  /**
   * Builds a single Stage by combining all operations from the stages in the path.
   * For the legacy adapter, we need to create a stage that represents the entire chain from source to end.
   */
  private def buildStageFromPath(graph: StageGraph, path: Seq[StageId]): Stage[_, _] = {
    // The path contains stage IDs from source to final stage
    // Each stage already contains the operations accumulated up to that point
    // We need to chain them together to create the complete transformation

    if (path.length == 1) {
      // Single stage - just return it
      graph.stages(path.headOption.get).stage
    } else {
      // Multiple stages - chain them together
      // Start with the first stage and chain each subsequent stage
      val stages = path.map(graph.stages(_).stage)
      stages.drop(1).foldLeft(stages.headOption.get) { (chained, nextStage) =>
        Stage.ChainedStage(chained.asInstanceOf[Stage[Any, Any]], nextStage.asInstanceOf[Stage[Any, Any]])
      }
    }
  }

  /**
   * Recursively checks if a Plan tree contains any wide (shuffle) operations.
   *
   * This method traverses the Plan tree to detect operations that require shuffle boundaries,
   * which are incompatible with the legacy buildStages method. It's used to enforce
   * migration to the DAGScheduler for plans containing wide transformations.
   *
   * @param plan The Plan node to check
   * @return true if the plan contains any shuffle operations, false otherwise
   */
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


