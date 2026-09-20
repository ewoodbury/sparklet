package com.ewoodbury.sparklet.execution

import scala.collection.mutable

import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf, StageId}

/**
 * Builds stage execution graphs from plans, handling both narrow transformations and shuffle
 * boundaries for wide transformations.
 *
 * Named erasure boundary: plan element types are erased when operations are accumulated into
 * stages, and materialization casts back to the closure types the executor runs. Safe because Plan
 * and Operation carry the same types for any given pipeline position.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.MutableDataStructures",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.RedundantAsInstanceOf",
  ),
)
object StageBuilder:

  /**
   * Describes how a stage's output is partitioned.
   *
   * `byKey = true` means all elements with equal keys are guaranteed to be in the same partition
   * AND the data was written through the key hash partitioner. It is set only by true shuffle
   * stages (groupByKey, reduceByKey, partitionBy, join, cogroup) and by bypassed local operations
   * chained after such a stage; narrow transformations merely preserve it. It must not be inferred
   * merely because records happen to be emitted in a stable order.
   */
  final case class Partitioning(byKey: Boolean, numPartitions: Int)

  /**
   * Information about a stage in the execution graph.
   */
  case class StageInfo(
      id: StageId,
      stage: Stage[_, _],
      inputSources: Seq[InputSource], // What this stage reads from
      isShuffleStage: Boolean,
      /** For shuffle stages, the wide operation this stage executes. */
      wideOp: Option[WideOp],
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
   * Immutable stage draft for accumulating operations during stage construction. All fields are
   * immutable; mutations create new instances.
   */
  private case class StageDraft(
      id: StageId,
      ops: Vector[Operation[Any, Any]],
      inputSources: Seq[InputSource],
      isShuffle: Boolean,
      /** Set exactly when `isShuffle` is true: the wide operation this stage executes. */
      shuffleMeta: Option[WideOp],
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
   * Encapsulated mutation helpers to centralize stage builder and dependency management. Provides
   * single points for invariant checking and reduces accidental misuse.
   */
  private def putBuilder(
      builderMap: mutable.Map[StageId, StageDraft],
      builder: StageDraft,
  ): Unit = {
    builderMap(builder.id) = builder
  }

  private def putNewBuilder(
      builderMap: mutable.Map[StageId, StageDraft],
      builder: StageDraft,
  ): Unit = {
    // Check invariant: no duplicate stage IDs
    require(
      !builderMap.contains(builder.id),
      s"Stage ID ${builder.id} already exists in builder map",
    )
    builderMap(builder.id) = builder
  }

  private def addDependency(
      dependencies: mutable.Map[StageId, mutable.Set[StageId]],
      child: StageId,
      parent: StageId,
  ): Unit = {
    require(child != parent, s"Stage $child cannot depend on itself")
    dependencies.getOrElseUpdate(child, mutable.Set.empty) += parent
  }

  /**
   * Builds a complete stage graph from a plan using the unified builder approach. This replaces
   * the old recursive approach with a more structured operation accumulation.
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
        wideOp = builder.shuffleMeta,
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
   * Validates StageGraph invariants to catch errors early. Checks for consistency issues that
   * could cause runtime failures.
   */
  private[execution] def validateStageGraph(graph: StageGraph): Unit = {
    // 1. finalStageId exists in stages map
    if (!graph.stages.contains(graph.finalStageId)) {
      val availableStages = graph.stages.keys.toSeq.sorted.mkString(", ")
      throw new IllegalStateException(
        s"Final stage ID ${graph.finalStageId} not found in stages map. " +
          s"Available stage IDs: [$availableStages]. " +
          "This indicates a stage graph construction error.",
      )
    }

    // 2. Every dependency target exists
    graph.dependencies.foreachEntry { (stageId, deps) =>
      if (!graph.stages.contains(stageId)) {
        val availableStages = graph.stages.keys.toSeq.sorted.mkString(", ")
        throw new IllegalStateException(
          s"Stage $stageId has dependencies but is not in stages map. " +
            s"Available stage IDs: [$availableStages]. " +
            s"Dependencies: [${deps.mkString(", ")}]",
        )
      }
      deps.foreach { depId =>
        if (!graph.stages.contains(depId)) {
          val availableStages = graph.stages.keys.toSeq.sorted.mkString(", ")
          throw new IllegalStateException(
            s"Stage $stageId depends on missing stage $depId. " +
              s"Available stage IDs: [$availableStages]. " +
              "Check stage construction order.",
          )
        }
      }
    }

    // 3. No stage lists itself as dependency (prevents infinite loops)
    graph.dependencies.foreachEntry { (stageId, deps) =>
      if (deps.contains(stageId)) {
        throw new IllegalStateException(
          s"Stage $stageId lists itself as a dependency, creating a self-cycle. " +
            s"All dependencies: [${deps.mkString(", ")}]. " +
            "This would cause infinite recursion during execution.",
        )
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
          throw new IllegalStateException(
            s"Stage ${stageInfo.id} has byKey=true but numPartitions=${partitioning.numPartitions} <= 0",
          )
        }
        // numPartitions should be reasonable (not excessively large)
        if (partitioning.numPartitions > 1000000) {
          throw new IllegalStateException(
            s"Stage ${stageInfo.id} has excessively large numPartitions=${partitioning.numPartitions}",
          )
        }
      }
    }
  }

  /**
   * Validates that the stage graph is acyclic using depth-first search.
   */
  private def validateAcyclicity(graph: StageGraph): Unit = {
    val visiting = mutable.Set[StageId]()
    val visited = mutable.Set[StageId]()

    def dfsVisit(stageId: StageId): Unit = {
      if (visiting.contains(stageId)) {
        val cyclePath = visiting.toSeq :+ stageId
        throw new IllegalStateException(
          s"Cycle detected in stage graph involving stage $stageId. " +
            s"Cycle path: ${cyclePath.mkString(" -> ")}. " +
            "This indicates a circular dependency that would prevent execution.",
        )
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
      val reachableStages = reachable.toSeq.sorted.mkString(", ")
      throw new IllegalStateException(
        s"Orphaned stages not reachable from finalStageId ${graph.finalStageId}: [${orphaned.toSeq.sorted.mkString(", ")}]. " +
          s"Reachable stages: [$reachableStages]. " +
          "Check for disconnected stage subgraphs or missing dependencies.",
      )
    }
  }

  /**
   * Validates that stage IDs form a monotonic sequence (strictly increasing from 0).
   */
  private def validateStageIdMonotonicity(graph: StageGraph): Unit = {
    val stageIds = graph.stages.keys.toSeq.sorted
    if (stageIds.nonEmpty) {
      // Check starts from 0
      stageIds.headOption match {
        case Some(firstId) if firstId.toInt != 0 =>
          throw new IllegalStateException(
            s"Stage IDs should start from 0, but found minimum ID: ${firstId.toInt}",
          )
        case None =>
          // This case shouldn't happen since we check nonEmpty above, but handle defensively
          throw new IllegalStateException("Stage ID sequence is unexpectedly empty")
        case Some(_) => // firstId.toInt == 0, which is correct
      }

      // Check for gaps (warn only to future-proof ID reuse scenarios)
      val expectedSequence = (0 until stageIds.length).map(StageId(_))
      val actualSet = stageIds.toSet
      val missing = expectedSequence.filterNot(actualSet.contains)
      if (missing.nonEmpty) {
        // Use println instead of logging to avoid dependencies
        println(
          s"Warning: Stage ID sequence has gaps. Missing IDs: ${missing.map(_.toInt).mkString(", ")}",
        )
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
        if (stageInfo.wideOp.isEmpty) {
          throw new IllegalStateException(
            s"Shuffle stage ${stageInfo.id} has no shuffle operation metadata",
          )
        }

        // Shuffle stages should have empty ops vector (they don't execute operations)
        stageInfo.stage match {
          case _: Stage.ChainedStage[_, _, _] =>
            // ChainedStage should not be used for shuffle stages
            throw new IllegalStateException(
              s"Shuffle stage ${stageInfo.id} incorrectly uses ChainedStage",
            )
          case _ => // Other stage types are acceptable for shuffle stages
        }

        // Multi-input shuffle stages should have proper side markers
        if (stageInfo.inputSources.length == 2) {
          val shuffleInputs = stageInfo.inputSources.collect { case si: ShuffleInput => si }
          if (shuffleInputs.length != 2) {
            throw new IllegalStateException(
              s"Multi-input shuffle stage ${stageInfo.id} should have exactly 2 ShuffleInputs, found ${shuffleInputs.length}",
            )
          }
          if (shuffleInputs.exists(_.side.isEmpty)) {
            throw new IllegalStateException(
              s"Multi-input shuffle stage ${stageInfo.id} has ShuffleInputs without side markers",
            )
          }
          val sides = shuffleInputs.flatMap(_.side).toSet
          if (sides.size != 2 || !sides.contains(Side.Left) || !sides.contains(Side.Right)) {
            throw new IllegalStateException(
              s"Multi-input shuffle stage ${stageInfo.id} has invalid side markers: expected {Left, Right}, found $sides",
            )
          }

          // Validate numPartitions consistency across inputs
          val numPartitionsList = shuffleInputs.map(_.numPartitions).distinct
          if (numPartitionsList.length > 1) {
            throw new IllegalStateException(
              s"Multi-input shuffle stage ${stageInfo.id} has mismatched numPartitions across inputs: ${numPartitionsList.mkString(", ")}",
            )
          }
        }
      }
    }
  }

  /**
   * Materializes a vector of operations into a concrete Stage form.
   *
   * This function converts a sequence of Operation ADT instances into executable Stage objects. It
   * employs several optimization strategies:
   *
   *   1. **Single Operation Optimization**: For single operations, returns the operation directly
   *      without unnecessary chaining to minimize overhead.
   *   2. **Efficient Chaining**: For multiple operations, uses a left fold to build the chain,
   *      creating ChainedStage instances that compose operations efficiently.
   *
   * @param ops
   *   Non-empty vector of operations to materialize
   * @return
   *   A concrete Stage that can be executed
   * @throws UnsupportedOperationException
   *   if the operation vector contains wide operations (shuffle operations) that cannot be
   *   materialized as narrow stages
   * @note
   *   This function only handles narrow operations. Wide operations should be processed through
   *   separate shuffle stages before materialization.
   */
  private[execution] def materialize(ops: Vector[Operation[Any, Any]]): Stage[Any, Any] = {
    require(ops.nonEmpty, "Cannot materialize empty operation vector")

    // For single operations, return the operation directly without chaining
    if (ops.length == 1) {
      ops.headOption match {
        case Some(op) => createStageFromOp(op)
        case None =>
          throw new IllegalStateException(
            "Single operation vector unexpectedly empty after length check. " +
              "This indicates a concurrent modification or internal error.",
          )
      }
    } else {
      // For multiple operations, build the chain efficiently
      ops.headOption match {
        case Some(firstOp) =>
          ops.drop(1).foldLeft(createStageFromOp(firstOp)) { (stage, op) =>
            chainOperationUnsafe(stage, op)
          }
        case None =>
          throw new IllegalStateException(
            "Multiple operation vector unexpectedly empty after validation. " +
              s"Original vector length: ${ops.length}. " +
              "This indicates a concurrent modification or internal error.",
          )
      }
    }
  }

  /**
   * Single controlled point of type erasure where it's unavoidable. This is the only place where
   * we deal with Any types and casting, keeping type safety elsewhere.
   */
  private def createStageFromOpUnsafe(op: Operation[Any, Any]): Stage[Any, Any] = {
    op match {
      case MapOp(f) => Stage.map(f)
      case FilterOp(p) => Stage.filter(p)
      case FlatMapOp(f) => Stage.flatMap(f)
      case DistinctOp() => Stage.distinct[Any]
      case MapPartitionsOp(f) => Stage.mapPartitions(f)

      // Bypassed wide ops are appended as no-op narrow stages: the upstream data is already
      // partitioned the way the operation wants it.
      case PartitionByLocalOp(_) => Stage.identity[Any]
      case _: RepartitionOp[_] => Stage.identity[Any]
      case _: CoalesceOp[_] => Stage.identity[Any]

      // Key-value operations - controlled type erasure point
      case _: KeysOp[_, _] =>
        Stage.keys[Any, Any].asInstanceOf[Stage[Any, Any]]
      case _: ValuesOp[_, _] =>
        Stage.values[Any, Any].asInstanceOf[Stage[Any, Any]]
      case MapValuesOp(f) =>
        Stage
          .mapValues[Any, Any, Any](f.asInstanceOf[Any => Any])
          .asInstanceOf[Stage[Any, Any]]
      case FilterKeysOp(p) =>
        Stage
          .filterKeys[Any, Any](p.asInstanceOf[Any => Boolean])
          .asInstanceOf[Stage[Any, Any]]
      case FilterValuesOp(p) =>
        Stage
          .filterValues[Any, Any](p.asInstanceOf[Any => Boolean])
          .asInstanceOf[Stage[Any, Any]]
      case FlatMapValuesOp(f) =>
        Stage
          .flatMapValues[Any, Any, Any](f.asInstanceOf[Any => IterableOnce[Any]])
          .asInstanceOf[Stage[Any, Any]]
      case _: GroupByKeyLocalOp[_, _] =>
        Stage.groupByKeyLocal[Any, Any].asInstanceOf[Stage[Any, Any]]
      case ReduceByKeyLocalOp(reduceFunc) =>
        Stage
          .reduceByKeyLocal[Any, Any](reduceFunc.asInstanceOf[(Any, Any) => Any])
          .asInstanceOf[Stage[Any, Any]]

      case _ => throw new UnsupportedOperationException(s"Cannot create stage from operation: $op")
    }
  }

  /**
   * Helper function to chain an operation onto an existing stage. This function encapsulates the
   * pattern matching for operation chaining and eliminates the need for unsafe type casting where
   * possible.
   */
  private def chainOperationUnsafe(
      stage: Stage[Any, Any],
      op: Operation[Any, Any],
  ): Stage[Any, Any] = {
    Stage.ChainedStage(stage, createStageFromOpUnsafe(op))
  }

  /**
   * Creates a concrete Stage object from a single Operation ADT instance.
   *
   * This method serves as the bridge between the high-level Operation ADT and the concrete Stage
   * implementations that can be executed by the runtime. It pattern matches on the operation type
   * and delegates to the appropriate Stage constructor.
   *
   * The method handles all narrow transformation operations including:
   *   - Basic transformations: MapOp, FilterOp, FlatMapOp, DistinctOp
   *   - Key-value operations: KeysOp, ValuesOp, MapValuesOp, FilterKeysOp, FilterValuesOp,
   *     FlatMapValuesOp
   *   - Partition operations: MapPartitionsOp
   *   - Local/bypass operations: GroupByKeyLocalOp, ReduceByKeyLocalOp, PartitionByLocalOp
   *
   * This method is optimized for creating individual stages and is more efficient than
   * materialize() when dealing with single operations, as it avoids unnecessary ChainedStage
   * wrapping.
   *
   * @param op
   *   The Operation to convert into a Stage
   * @return
   *   A concrete Stage[Any, Any] that can be executed
   * @throws UnsupportedOperationException
   *   if the operation is a wide operation that requires shuffle boundaries
   * @note
   *   This method only supports narrow operations. Wide operations should be handled through
   *   shuffle stages created by createShuffleStageUnified().
   */
  private def createStageFromOp(op: Operation[Any, Any]): Stage[Any, Any] = {
    createStageFromOpUnsafe(op)
  }

  /**
   * Core recursive method that traverses a Plan tree and builds stage graphs with operation
   * accumulation.
   *
   * This method implements the unified stage building algorithm that processes Plan nodes
   * recursively, accumulating narrow operations into stages and creating shuffle boundaries for
   * wide operations.
   *
   * The method handles three main categories of Plan nodes:
   *
   *   1. **Data Sources (Plan.Source)**:
   *      - Base case that creates initial stage builders for data partitions
   *      - Sets up source partitioning metadata based on partition count
   *      - Returns the stage ID and original Plan for dependency tracking
   *   2. **Narrow Transformations**:
   *      - Accumulates operations using appendOperation() when possible
   *      - Creates new stages when chaining is not feasible
   *      - Handles: MapOp, FilterOp, FlatMapOp, DistinctOp, KeysOp, ValuesOp, MapValuesOp,
   *        FilterKeysOp, FilterValuesOp, FlatMapValuesOp, MapPartitionsOp
   *   3. **Multi-input Operations**:
   *      - UnionOp: Creates a new stage that reads from both input stages
   *      - Preserves no partitioning metadata due to input consolidation
   *   4. **Wide Transformations (Shuffle Operations)**:
   *      - GroupByKeyOp, ReduceByKeyOp: Supports shuffle bypass optimization when already
   *        partitioned correctly
   *      - SortByOp, PartitionByOp, RepartitionOp, CoalesceOp: Always creates shuffle boundaries
   *      - JoinOp, CoGroupOp: Multi-input wide operations with side tagging for disambiguation
   *      - Uses WideOp metadata structures for shuffle execution planning
   *
   * **Optimization Strategies**:
   *   - Shuffle Bypass: For operations like groupByKey/reduceByKey, checks if upstream
   *     partitioning matches requirements and bypasses shuffle when possible
   *   - Operation Chaining: Accumulates multiple narrow operations into single stages to reduce
   *     overhead
   *   - Dependency Tracking: Builds comprehensive dependency graph for execution ordering
   *
   * @param ctx
   *   BuildContext for generating unique stage IDs
   * @param plan
   *   The Plan node to process recursively
   * @param builderMap
   *   Mutable map accumulating stage builders (modified in place)
   * @param dependencies
   *   Mutable map tracking stage dependencies (modified in place)
   * @tparam A
   *   Type parameter of the Plan (preserved for type safety)
   * @return
   *   Tuple of (StageId, Option[Plan]) where StageId is the resulting stage identifier, and
   *   Option[Plan] contains the original Plan for shuffle operation metadata
   * @note
   *   This method modifies builderMap and dependencies in place for efficiency
   * @note
   *   The return Plan is crucial for shuffle stages to preserve original operation metadata
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
        putNewBuilder(
          builderMap,
          StageDraft(
            id = stageId,
            ops = Vector.empty[
              Operation[Any, Any],
            ], // No operations for source - operations will be chained here
            inputSources = Seq(SourceInput(source.partitions)),
            isShuffle = false,
            shuffleMeta = None,
            outputPartitioning =
              Some(Partitioning(byKey = false, numPartitions = source.partitions.size)),
          ),
        )
        (stageId, Some(source))

      // Narrow transformations - accumulate operations or create new stages
      case Plan.MapOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(
          ctx,
          sourceStageId,
          Operation.fromPlan(plan),
          builderMap,
          dependencies,
        )
        (resultId, Some(plan))

      case Plan.FilterOp(sourcePlan, p) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(
          ctx,
          sourceStageId,
          Operation.fromPlan(plan),
          builderMap,
          dependencies,
        )
        (resultId, Some(plan))

      case Plan.FlatMapOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(
          ctx,
          sourceStageId,
          Operation.fromPlan(plan),
          builderMap,
          dependencies,
        )
        (resultId, Some(plan))

      case Plan.DistinctOp(sourcePlan) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(
          ctx,
          sourceStageId,
          Operation.fromPlan(plan),
          builderMap,
          dependencies,
        )
        (resultId, Some(plan))

      case Plan.KeysOp(sourcePlan) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(
          ctx,
          sourceStageId,
          Operation.fromPlan(plan),
          builderMap,
          dependencies,
        )
        (resultId, Some(plan))

      case Plan.ValuesOp(sourcePlan) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(
          ctx,
          sourceStageId,
          Operation.fromPlan(plan),
          builderMap,
          dependencies,
        )
        (resultId, Some(plan))

      case Plan.MapValuesOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(
          ctx,
          sourceStageId,
          Operation.fromPlan(plan),
          builderMap,
          dependencies,
        )
        (resultId, Some(plan))

      case Plan.FilterKeysOp(sourcePlan, p) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(
          ctx,
          sourceStageId,
          Operation.fromPlan(plan),
          builderMap,
          dependencies,
        )
        (resultId, Some(plan))

      case Plan.FilterValuesOp(sourcePlan, p) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(
          ctx,
          sourceStageId,
          Operation.fromPlan(plan),
          builderMap,
          dependencies,
        )
        (resultId, Some(plan))

      case Plan.FlatMapValuesOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(
          ctx,
          sourceStageId,
          Operation.fromPlan(plan),
          builderMap,
          dependencies,
        )
        (resultId, Some(plan))

      case Plan.MapPartitionsOp(sourcePlan, f) =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sourcePlan, builderMap, dependencies)
        val resultId = appendOperation(
          ctx,
          sourceStageId,
          Operation.fromPlan(plan),
          builderMap,
          dependencies,
        )
        (resultId, Some(plan))

      case Plan.UnionOp(left, right) =>
        val (leftStageId, _) = buildStagesFromPlan(ctx, left, builderMap, dependencies)
        val (rightStageId, _) = buildStagesFromPlan(ctx, right, builderMap, dependencies)

        // Union creates a new narrow stage that reads outputs from both input stages
        val unionStageId = ctx.freshId()
        putNewBuilder(
          builderMap,
          StageDraft(
            id = unionStageId,
            ops = Vector.empty[Operation[Any, Any]], // No operations, just union of inputs
            inputSources = Seq(StageOutput(leftStageId), StageOutput(rightStageId)),
            isShuffle = false,
            shuffleMeta = None,
            outputPartitioning = None, // Union doesn't preserve partitioning
          ),
        )

        addDependency(dependencies, unionStageId, leftStageId)
        addDependency(dependencies, unionStageId, rightStageId)
        (unionStageId, Some(plan))

      // Wide transformations (create shuffle boundaries)
      case groupByKey: Plan.GroupByKeyOp[_, _] =>
        val (sourceStageId, _) =
          buildStagesFromPlan(ctx, groupByKey.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)
        val defaultN = SparkletConf.get.defaultShufflePartitions

        if (Operation.canBypassShuffle(groupByKey, src.outputPartitioning, SparkletConf.get)) {
          // Add local groupByKey operation to existing stage since shuffle can be bypassed
          val resultId = appendOperation(
            ctx,
            sourceStageId,
            GroupByKeyLocalOp[Any, Any]().asInstanceOf[Operation[Any, Any]],
            builderMap,
            dependencies,
          )
          (resultId, Some(groupByKey))
        } else {
          val shuffleId = createShuffleStageUnified(
            ctx,
            Seq(sourceStageId),
            GroupByKeyWideOp(
              SimpleWideOpMeta(
                kind = WideOpKind.GroupByKey,
                numPartitions = defaultN,
              ),
            ),
            builderMap,
            dependencies,
          )
          (shuffleId, Some(groupByKey))
        }

      case reduceByKey: Plan.ReduceByKeyOp[_, _] =>
        val (sourceStageId, _) =
          buildStagesFromPlan(ctx, reduceByKey.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)
        val defaultN = SparkletConf.get.defaultShufflePartitions

        if (Operation.canBypassShuffle(reduceByKey, src.outputPartitioning, SparkletConf.get)) {
          // Add local reduceByKey operation to existing stage since shuffle can be bypassed
          val resultId = appendOperation(
            ctx,
            sourceStageId,
            ReduceByKeyLocalOp[Any, Any](reduceByKey.reduceFunc.asInstanceOf[(Any, Any) => Any])
              .asInstanceOf[Operation[Any, Any]],
            builderMap,
            dependencies,
          )
          (resultId, Some(reduceByKey))
        } else {
          val shuffleId = createShuffleStageUnified(
            ctx,
            Seq(sourceStageId),
            ReduceByKeyWideOp(
              ReduceWideOpMeta(
                numPartitions = defaultN,
                reduceFunc = reduceByKey.reduceFunc,
              ),
            ),
            builderMap,
            dependencies,
          )
          (shuffleId, Some(reduceByKey))
        }

      case sortBy: Plan.SortByOp[_, _] =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, sortBy.source, builderMap, dependencies)
        val n = SparkletConf.get.defaultShufflePartitions
        val shuffleId = createShuffleStageUnified(
          ctx,
          Seq(sourceStageId),
          SortByWideOp(
            SortWideOpMeta(
              numPartitions = n,
              keyFunc = sortBy.keyFunc,
              ordering = sortBy.ordering,
            ),
          ),
          builderMap,
          dependencies,
        )
        (shuffleId, Some(sortBy))

      case pby: Plan.PartitionByOp[_, _] =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, pby.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)

        if (Operation.canBypassShuffle(pby, src.outputPartitioning, SparkletConf.get)) {
          val resultId = appendOperation(
            ctx,
            sourceStageId,
            PartitionByLocalOp[Any, Any](pby.numPartitions).asInstanceOf[Operation[Any, Any]],
            builderMap,
            dependencies,
          )
          (resultId, Some(pby))
        } else {
          val shuffleId = createShuffleStageUnified(
            ctx,
            Seq(sourceStageId),
            PartitionByWideOp(
              SimpleWideOpMeta(
                kind = WideOpKind.PartitionBy,
                numPartitions = pby.numPartitions,
              ),
            ),
            builderMap,
            dependencies,
          )
          (shuffleId, Some(pby))
        }

      case rep: Plan.RepartitionOp[_] =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, rep.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)

        if (Operation.canBypassShuffle(rep, src.outputPartitioning, SparkletConf.get)) {
          val resultId = appendOperation(
            ctx,
            sourceStageId,
            RepartitionOp[Any](rep.numPartitions),
            builderMap,
            dependencies,
          )
          (resultId, Some(rep))
        } else {
          val shuffleId = createShuffleStageUnified(
            ctx,
            Seq(sourceStageId),
            RepartitionWideOp(
              SimpleWideOpMeta(
                kind = WideOpKind.Repartition,
                numPartitions = rep.numPartitions,
              ),
            ),
            builderMap,
            dependencies,
          )
          (shuffleId, Some(rep))
        }

      case coal: Plan.CoalesceOp[_] =>
        val (sourceStageId, _) = buildStagesFromPlan(ctx, coal.source, builderMap, dependencies)
        val src = builderMap(sourceStageId)

        if (Operation.canBypassShuffle(coal, src.outputPartitioning, SparkletConf.get)) {
          val resultId = appendOperation(
            ctx,
            sourceStageId,
            CoalesceOp[Any](coal.numPartitions),
            builderMap,
            dependencies,
          )
          (resultId, Some(coal))
        } else {
          val shuffleId = createShuffleStageUnified(
            ctx,
            Seq(sourceStageId),
            CoalesceWideOp(
              SimpleWideOpMeta(
                kind = WideOpKind.Coalesce,
                numPartitions = coal.numPartitions,
              ),
            ),
            builderMap,
            dependencies,
          )
          (shuffleId, Some(coal))
        }

      case joinOp: Plan.JoinOp[_, _, _] =>
        val (leftStageId, _) = buildStagesFromPlan(ctx, joinOp.left, builderMap, dependencies)
        val (rightStageId, _) = buildStagesFromPlan(ctx, joinOp.right, builderMap, dependencies)

        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val shuffleId = createShuffleStageUnified(
          ctx,
          Seq(leftStageId, rightStageId),
          JoinWideOp(
            SimpleWideOpMeta(
              kind = WideOpKind.Join,
              numPartitions = numPartitions,
              joinStrategy = joinOp.joinStrategy,
              sides = Seq(Side.Left, Side.Right),
            ),
          ),
          builderMap,
          dependencies,
        )
        (shuffleId, Some(joinOp))

      case cogroupOp: Plan.CoGroupOp[_, _, _] =>
        val (leftStageId, _) = buildStagesFromPlan(ctx, cogroupOp.left, builderMap, dependencies)
        val (rightStageId, _) = buildStagesFromPlan(ctx, cogroupOp.right, builderMap, dependencies)

        val numPartitions = SparkletConf.get.defaultShufflePartitions
        val shuffleId = createShuffleStageUnified(
          ctx,
          Seq(leftStageId, rightStageId),
          CoGroupWideOp(
            SimpleWideOpMeta(
              kind = WideOpKind.CoGroup,
              numPartitions = numPartitions,
              sides = Seq(Side.Left, Side.Right),
            ),
          ),
          builderMap,
          dependencies,
        )
        (shuffleId, Some(cogroupOp))
    }
  }

  /**
   * Appends a narrow operation to an existing stage or creates a new stage if needed. This handles
   * the logic of when operations can be chained vs when new stages are required. Returns the
   * StageId that will produce the result (could be the source stage or a new one).
   */
  private def appendOperation(
      ctx: BuildContext,
      sourceStageId: StageId,
      op: Operation[Any, Any],
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
        outputPartitioning = updatePartitioning(sourceBuilder.outputPartitioning, op),
      )
      putNewBuilder(builderMap, newBuilder)

      // Add dependency: new stage depends on source stage
      addDependency(dependencies, newStageId, sourceStageId)
      newStageId
    } else {
      // Narrow ops can chain onto any non-shuffle stage: ops run per partition of the stage's
      // output, which is valid for single-input stages and for concatenation stages (union).
      val canChain = sourceBuilder.inputSources.forall {
        case _: SourceInput => true
        case _: StageOutput => true
        case _: ShuffleInput => false
      }

      if (canChain) {
        // Extend the existing stage by appending the operation
        val updatedOps = sourceBuilder.ops :+ op
        val updatedBuilder = sourceBuilder.copy(
          ops = updatedOps,
          outputPartitioning = updatePartitioning(sourceBuilder.outputPartitioning, op),
        )
        putBuilder(builderMap, updatedBuilder)
        sourceStageId
      } else {
        // Create a new stage that reads the source stage's computed output
        val newStageId = ctx.freshId()
        val newBuilder = StageDraft(
          id = newStageId,
          ops = Vector(op),
          inputSources = Seq(StageOutput(sourceStageId)),
          isShuffle = false,
          shuffleMeta = None,
          outputPartitioning = updatePartitioning(sourceBuilder.outputPartitioning, op),
        )
        putNewBuilder(builderMap, newBuilder)
        addDependency(dependencies, newStageId, sourceStageId)
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
          ShuffleInput(upstreamIds(1), Some(meta.sides(1)), meta.numPartitions),
        )
      case _ =>
        // Single-input operations
        require(upstreamIds.length == 1, s"${meta.kind} requires exactly 1 upstream stage")
        upstreamIds.headOption match {
          case Some(upstreamId) => Seq(ShuffleInput(upstreamId, None, meta.numPartitions))
          case None =>
            throw new IllegalStateException(
              s"${meta.kind} operation missing required upstream stage",
            )
        }
    }

    // Determine output partitioning based on operation type
    val outputPartitioning = meta.kind match {
      case WideOpKind.GroupByKey | WideOpKind.ReduceByKey | WideOpKind.PartitionBy |
          WideOpKind.Join | WideOpKind.CoGroup =>
        Some(Partitioning(byKey = true, numPartitions = meta.numPartitions))
      case WideOpKind.SortBy =>
        Some(Partitioning(byKey = false, numPartitions = meta.numPartitions))
      case WideOpKind.Repartition | WideOpKind.Coalesce =>
        Some(Partitioning(byKey = false, numPartitions = meta.numPartitions))
    }

    putNewBuilder(
      builderMap,
      StageDraft(
        id = shuffleStageId,
        ops = Vector.empty[Operation[Any, Any]], // Shuffle stages don't have narrow operations
        inputSources = shuffleInputSources,
        isShuffle = true,
        shuffleMeta = Some(wideOp),
        outputPartitioning = outputPartitioning,
      ),
    )

    // Add dependencies for all upstream stages
    upstreamIds.foreach { upstreamId =>
      addDependency(dependencies, shuffleStageId, upstreamId)
    }

    shuffleStageId
  }

  // --- Metadata helpers ---

  /**
   * Updates output partitioning based on a narrow operation's transformation semantics.
   *
   * This function centralizes the logic for how each operation type affects partitioning metadata,
   * ensuring consistent behavior across the stage building process.
   *
   * **Operation Categories and Effects:**
   *
   *   1. **Preserve completely**: MapOp, FilterOp, FlatMapOp, MapPartitionsOp
   *      - Maintains both `byKey` status and `numPartitions`
   *   2. **Preserve count, modify key awareness**:
   *      - KeysOp: Sets `byKey = false` (output is key-only, not key-value pairs)
   *      - ValuesOp: Sets `byKey = false` (output is value-only)
   *      - MapValuesOp, FilterKeysOp, FilterValuesOp, FlatMapValuesOp: Preserve both
   *   3. **Key awareness conditional**: DistinctOp
   *      - Preserves `byKey = true` if input was key-partitioned
   *      - Sets `byKey = false` otherwise
   *   4. **Local bypass operations**: GroupByKeyLocalOp, ReduceByKeyLocalOp
   *      - Preserve existing partitioning (no shuffle occurred)
   *   5. **Bypassed wide operations**: Establish new partitioning based on operation semantics;
   *      true shuffle stages set their output partitioning in `createShuffleStageUnified` instead
   *
   * @param prev
   *   Previous partitioning metadata from the upstream stage
   * @param op
   *   The operation being applied to transform the data
   * @return
   *   Updated partitioning metadata reflecting the operation's effect
   *
   * @note
   *   This method handles the operations that can be appended to narrow stages. If a new operation
   *   is added to the Operation ADT, this method must be updated to handle it.
   */
  private def updatePartitioning(
      prev: Option[Partitioning],
      op: Operation[Any, Any],
  ): Option[Partitioning] = {
    val result = op match {
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

      case _: MapValuesOp[_, _, _] | _: FilterKeysOp[_, _] | _: FilterValuesOp[_, _] |
          _: FlatMapValuesOp[_, _, _] =>
        // These preserve the partitioning structure
        prev

      case _: DistinctOp[_] =>
        /* Distinct clears byKey unless previous was byKey (preserves key-based partitioning for
         * key-value data) */
        prev.map(p => if (p.byKey) p else p.copy(byKey = false))

      // Local (bypassed) operations preserve or establish key-based partitioning
      case _: GroupByKeyLocalOp[_, _] | _: ReduceByKeyLocalOp[_, _] =>
        prev

      case pbl: PartitionByLocalOp[_, _] =>
        Some(Partitioning(byKey = true, numPartitions = pbl.numPartitions))

      case rep: RepartitionOp[_] =>
        Some(Partitioning(byKey = false, numPartitions = rep.numPartitions))

      case coal: CoalesceOp[_] =>
        Some(Partitioning(byKey = false, numPartitions = coal.numPartitions))
    }

    // Validation: ensure result is sensible
    result.foreach { partitioning =>
      require(
        partitioning.numPartitions > 0,
        s"Invalid partitioning from operation $op: numPartitions must be > 0, got ${partitioning.numPartitions}",
      )
      require(
        partitioning.numPartitions <= 1000000,
        s"Invalid partitioning from operation $op: numPartitions ${partitioning.numPartitions} exceeds reasonable limit",
      )
    }

    result
  }
