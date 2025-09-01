package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.{Plan, PlanWide}

/**
 * Internal representation of operations that can be applied to data in a stage. This ADT replaces
 * the nested Stage.ChainedStage approach for better introspection, optimization, and future code
 * generation.
 *
 * @tparam A
 *   Input type of the operation
 * @tparam B
 *   Output type of the operation
 */
sealed trait Operation[A, B]

/**
 * Type-safe metadata for wide operations using sealed trait hierarchy.
 */
sealed trait WideOpMeta {
  def kind: WideOpKind
  def numPartitions: Int
  def sides: Seq[StageBuilder.Side]
}

/**
 * Metadata for simple wide operations that don't require functions.
 */
final case class SimpleWideOpMeta(
    kind: WideOpKind,
    numPartitions: Int,
    joinStrategy: Option[com.ewoodbury.sparklet.core.Plan.JoinStrategy] = None,
    sides: Seq[StageBuilder.Side] = Seq.empty,
) extends WideOpMeta

/**
 * Metadata for sort operations with typed key function.
 */
final case class SortWideOpMeta[A, B](
    kind: WideOpKind = WideOpKind.SortBy,
    numPartitions: Int,
    keyFunc: A => B,
    sides: Seq[StageBuilder.Side] = Seq.empty,
) extends WideOpMeta

/**
 * Metadata for reduce operations with typed reduce function.
 */
final case class ReduceWideOpMeta[V](
    kind: WideOpKind = WideOpKind.ReduceByKey,
    numPartitions: Int,
    reduceFunc: (V, V) => V,
    sides: Seq[StageBuilder.Side] = Seq.empty,
) extends WideOpMeta

/**
 * Enumeration of wide operation types that require shuffle boundaries.
 */
enum WideOpKind:
  case GroupByKey, ReduceByKey, SortBy, PartitionBy, Repartition, Coalesce, Join, CoGroup

/**
 * Structured representation of wide operations for execution planning. Replaces the raw Plan[_]
 * storage in StageInfo for better type safety and introspection.
 */
sealed trait WideOp

// Concrete wide operation implementations
final case class GroupByKeyWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class ReduceByKeyWideOp[V](meta: ReduceWideOpMeta[V]) extends WideOp
final case class SortByWideOp[A, B](meta: SortWideOpMeta[A, B]) extends WideOp
final case class PartitionByWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class RepartitionWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class CoalesceWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class JoinWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class CoGroupWideOp(meta: SimpleWideOpMeta) extends WideOp

// Narrow transformations
final case class MapOp[A, B](f: A => B) extends Operation[A, B]
final case class FilterOp[A](p: A => Boolean) extends Operation[A, A]
final case class FlatMapOp[A, B](f: A => IterableOnce[B]) extends Operation[A, B]
final case class DistinctOp[A]() extends Operation[A, A]
final case class KeysOp[K, V]() extends Operation[(K, V), K]
final case class ValuesOp[K, V]() extends Operation[(K, V), V]
final case class MapValuesOp[K, V, V2](f: V => V2) extends Operation[(K, V), (K, V2)]
final case class FilterKeysOp[K, V](p: K => Boolean) extends Operation[(K, V), (K, V)]
final case class FilterValuesOp[K, V](p: V => Boolean) extends Operation[(K, V), (K, V)]
final case class FlatMapValuesOp[K, V, V2](f: V => IterableOnce[V2])
    extends Operation[(K, V), (K, V2)]
final case class MapPartitionsOp[A, B](f: Iterator[A] => Iterator[B]) extends Operation[A, B]

// Wide transformations (shuffle operations) - simplified for now, will be filled in by builder
final case class GroupByKeyOp[K, V](numPartitions: Int) extends Operation[(K, V), (K, Iterable[V])]
final case class ReduceByKeyOp[K, V](reduceFunc: (V, V) => V, numPartitions: Int)
    extends Operation[(K, V), (K, V)]

// Local operations for bypassed shuffles (narrow operations that work on already-partitioned data)
final case class GroupByKeyLocalOp[K, V]() extends Operation[(K, V), (K, Iterable[V])]
final case class ReduceByKeyLocalOp[K, V](reduceFunc: (V, V) => V)
    extends Operation[(K, V), (K, V)]
final case class PartitionByLocalOp[K, V](numPartitions: Int) extends Operation[(K, V), (K, V)]
final case class SortByOp[A, S](keyFunc: A => S, numPartitions: Int) extends Operation[A, A]
final case class PartitionByOp[K, V](numPartitions: Int) extends Operation[(K, V), (K, V)]
final case class RepartitionOp[A](numPartitions: Int) extends Operation[A, A]
final case class CoalesceOp[A](numPartitions: Int) extends Operation[A, A]
final case class JoinOp[K, V, W](numPartitions: Int) extends Operation[(K, V), (K, (V, W))]
final case class CoGroupOp[K, V, W](numPartitions: Int)
    extends Operation[(K, V), (K, (Iterable[V], Iterable[W]))]

/**
 * Companion object with helper methods for working with operations.
 */
object Operation {

  /**
   * Determines if an operation requires a shuffle boundary.
   */
  private[execution] def needsShuffle(op: Operation[_, _]): Boolean = op match {
    case _: GroupByKeyOp[_, _] | _: ReduceByKeyOp[_, _] | _: SortByOp[_, _] |
        _: PartitionByOp[_, _] | _: RepartitionOp[_] | _: CoalesceOp[_] | _: JoinOp[_, _, _] |
        _: CoGroupOp[_, _, _] =>
      true
    case _ => false
  }

  /**
   * Determines if a Plan operation requires a shuffle boundary. This is the main function for
   * shuffle boundary detection during stage building.
   */
  private[execution] def needsShuffle(plan: Plan[_]): Boolean = {
    PlanWide.isDirectlyWide(plan)
  }

  /**
   * Optimization hook to determine if a shuffle operation can be bypassed based on upstream
   * partitioning metadata. This generalizes the current groupByKey/reduceByKey shortcut.
   */
  private[execution] def canBypassShuffle(
      plan: Plan[_],
      upstreamPartitioning: Option[com.ewoodbury.sparklet.execution.StageBuilder.Partitioning],
      conf: com.ewoodbury.sparklet.core.SparkletConf,
  ): Boolean = {
    plan match {
      case gbk: Plan.GroupByKeyOp[_, _] =>
        // Can bypass if already partitioned by key with correct partition count
        upstreamPartitioning.exists(p =>
          p.byKey && p.numPartitions == conf.defaultShufflePartitions,
        )

      case rbk: Plan.ReduceByKeyOp[_, _] =>
        // Can bypass if already partitioned by key with correct partition count
        upstreamPartitioning.exists(p =>
          p.byKey && p.numPartitions == conf.defaultShufflePartitions,
        )

      case pby: Plan.PartitionByOp[_, _] =>
        /* Can bypass PartitionBy if upstream is already partitioned by key with correct partition
         * count */
        // This enables chaining: partitionBy -> groupByKey to be optimized to a single stage
        upstreamPartitioning.exists(p => p.byKey && p.numPartitions == pby.numPartitions)

      case rep: Plan.RepartitionOp[_] =>
        // Can bypass if already has the desired partitioning
        upstreamPartitioning.exists(p => !p.byKey && p.numPartitions == rep.numPartitions)

      case coal: Plan.CoalesceOp[_] =>
        /* Coalesce always requires shuffle - cannot bypass even if partition count is already
         * correct */
        // because it may need to redistribute data across partitions
        false

      case _ =>
        // Other wide operations cannot bypass shuffle
        false
    }
  }

  /**
   * Creates an Operation from a Plan node. Note: This is a temporary adapter - eventually Plan
   * nodes will be converted to Operations directly. Due to type erasure at the Plan boundary, we
   * return Operation[Any, Any] but the underlying operation instances maintain their proper types.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private[execution] def fromPlan(plan: Plan[_]): Operation[Any, Any] = plan match {
    case Plan.MapOp(_, f) => MapOp(f.asInstanceOf[Any => Any])
    case Plan.FilterOp(_, p) => FilterOp(p.asInstanceOf[Any => Boolean])
    case Plan.FlatMapOp(_, f) => FlatMapOp(f.asInstanceOf[Any => IterableOnce[Any]])
    case Plan.DistinctOp(_) => DistinctOp[Any]()
    case Plan.KeysOp(_) => KeysOp[Any, Any]().asInstanceOf[Operation[Any, Any]]
    case Plan.ValuesOp(_) => ValuesOp[Any, Any]().asInstanceOf[Operation[Any, Any]]
    case Plan.MapValuesOp(_, f) =>
      MapValuesOp[Any, Any, Any](f.asInstanceOf[Any => Any]).asInstanceOf[Operation[Any, Any]]
    case Plan.FilterKeysOp(_, p) =>
      FilterKeysOp[Any, Any](p.asInstanceOf[Any => Boolean]).asInstanceOf[Operation[Any, Any]]
    case Plan.FilterValuesOp(_, p) =>
      FilterValuesOp[Any, Any](p.asInstanceOf[Any => Boolean]).asInstanceOf[Operation[Any, Any]]
    case Plan.FlatMapValuesOp(_, f) =>
      FlatMapValuesOp[Any, Any, Any](f.asInstanceOf[Any => IterableOnce[Any]])
        .asInstanceOf[Operation[Any, Any]]
    case Plan.MapPartitionsOp(_, f) =>
      MapPartitionsOp(f.asInstanceOf[Iterator[Any] => Iterator[Any]])
    case Plan.GroupByKeyOp(_) => GroupByKeyOp[Any, Any](0).asInstanceOf[Operation[Any, Any]]
    case Plan.ReduceByKeyOp(_, reduceFunc) =>
      ReduceByKeyOp[Any, Any](reduceFunc.asInstanceOf[(Any, Any) => Any], 0)
        .asInstanceOf[Operation[Any, Any]]
    case Plan.SortByOp(_, keyFunc, _) => SortByOp[Any, Any](keyFunc.asInstanceOf[Any => Any], 0)
    case Plan.PartitionByOp(_, numPartitions) =>
      PartitionByOp[Any, Any](numPartitions).asInstanceOf[Operation[Any, Any]]
    case Plan.RepartitionOp(_, numPartitions) => RepartitionOp[Any](numPartitions)
    case Plan.CoalesceOp(_, numPartitions) => CoalesceOp[Any](numPartitions)
    case Plan.JoinOp(_, _, _) => JoinOp[Any, Any, Any](0).asInstanceOf[Operation[Any, Any]]
    case Plan.CoGroupOp(_, _) => CoGroupOp[Any, Any, Any](0).asInstanceOf[Operation[Any, Any]]
    case _ => throw new UnsupportedOperationException(s"Cannot convert $plan to Operation")
  }
}
