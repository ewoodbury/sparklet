package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.Plan

/**
 * Internal representation of operations that can be applied to data in a stage.
 * This ADT replaces the nested Stage.ChainedStage approach for better introspection,
 * optimization, and future code generation.
 */
sealed trait Operation

/**
 * Metadata for wide operations (shuffle operations) that require special handling.
 * This encapsulates all the information needed to execute a wide transformation.
 */
final case class WideOpMeta(
    kind: WideOpKind,
    numPartitions: Int,
    keyFunc: Option[Any => Any] = None,
    reduceFunc: Option[(Any, Any) => Any] = None,
    joinStrategy: Option[com.ewoodbury.sparklet.core.Plan.JoinStrategy] = None,
    sides: Seq[com.ewoodbury.sparklet.execution.StageBuilder.Side] = Seq.empty
)

/**
 * Enumeration of wide operation types that require shuffle boundaries.
 */
enum WideOpKind:
  case GroupByKey, ReduceByKey, SortBy, PartitionBy, Repartition, Coalesce, Join, CoGroup

/**
 * Structured representation of wide operations for execution planning.
 * Replaces the raw Plan[_] storage in StageInfo for better type safety and introspection.
 */
sealed trait WideOp

// Concrete wide operation implementations
final case class GroupByKeyWideOp(meta: WideOpMeta) extends WideOp
final case class ReduceByKeyWideOp(meta: WideOpMeta) extends WideOp
final case class SortByWideOp(meta: WideOpMeta) extends WideOp
final case class PartitionByWideOp(meta: WideOpMeta) extends WideOp
final case class RepartitionWideOp(meta: WideOpMeta) extends WideOp
final case class CoalesceWideOp(meta: WideOpMeta) extends WideOp
final case class JoinWideOp(meta: WideOpMeta) extends WideOp
final case class CoGroupWideOp(meta: WideOpMeta) extends WideOp

// Narrow transformations
final case class MapOp[A, B](f: A => B) extends Operation
final case class FilterOp[A](p: A => Boolean) extends Operation
final case class FlatMapOp[A, B](f: A => IterableOnce[B]) extends Operation
final case class DistinctOp() extends Operation
final case class KeysOp[K, V]() extends Operation
final case class ValuesOp[K, V]() extends Operation
final case class MapValuesOp[K, V, V2](f: V => V2) extends Operation
final case class FilterKeysOp[K, V](p: K => Boolean) extends Operation
final case class FilterValuesOp[K, V](p: V => Boolean) extends Operation
final case class FlatMapValuesOp[K, V, V2](f: V => IterableOnce[V2]) extends Operation
final case class MapPartitionsOp[A, B](f: Iterator[A] => Iterator[B]) extends Operation

// Wide transformations (shuffle operations) - simplified for now, will be filled in by builder
final case class GroupByKeyOp[K, V](numPartitions: Int) extends Operation
final case class ReduceByKeyOp[K, V](reduceFunc: (V, V) => V, numPartitions: Int) extends Operation

// Local operations for bypassed shuffles (narrow operations that work on already-partitioned data)
final case class GroupByKeyLocalOp[K, V]() extends Operation
final case class ReduceByKeyLocalOp[K, V](reduceFunc: (V, V) => V) extends Operation
final case class PartitionByLocalOp[K, V](numPartitions: Int) extends Operation
final case class SortByOp[K, V](keyFunc: V => K, numPartitions: Int) extends Operation
final case class PartitionByOp[K, V](numPartitions: Int) extends Operation
final case class RepartitionOp[A](numPartitions: Int) extends Operation
final case class CoalesceOp[A](numPartitions: Int) extends Operation
final case class JoinOp[K, V, W](numPartitions: Int) extends Operation
final case class CoGroupOp[K, V, W](numPartitions: Int) extends Operation

/**
 * Companion object with helper methods for working with operations.
 */
object Operation {
  /**
   * Determines if an operation requires a shuffle boundary.
   */
  private[execution] def needsShuffle(op: Operation): Boolean = op match {
    case _: GroupByKeyOp[_, _] | _: ReduceByKeyOp[_, _] | _: SortByOp[_, _] |
         _: PartitionByOp[_, _] | _: RepartitionOp[_] | _: CoalesceOp[_] |
         _: JoinOp[_, _, _] | _: CoGroupOp[_, _, _] => true
    case _ => false
  }

  /**
   * Determines if a Plan operation requires a shuffle boundary.
   * This is the main function for shuffle boundary detection during stage building.
   */
  private[execution] def needsShuffle(plan: Plan[_]): Boolean = plan match {
    case _: Plan.GroupByKeyOp[_, _] | _: Plan.ReduceByKeyOp[_, _] | _: Plan.SortByOp[_, _] |
         _: Plan.PartitionByOp[_, _] | _: Plan.RepartitionOp[_] | _: Plan.CoalesceOp[_] |
         _: Plan.JoinOp[_, _, _] | _: Plan.CoGroupOp[_, _, _] => true
    case _ => false
  }

  /**
   * Optimization hook to determine if a shuffle operation can be bypassed based on
   * upstream partitioning metadata. This generalizes the current groupByKey/reduceByKey shortcut.
   */
  private[execution] def canBypassShuffle(plan: Plan[_], upstreamPartitioning: Option[com.ewoodbury.sparklet.execution.StageBuilder.Partitioning], conf: com.ewoodbury.sparklet.core.SparkletConf): Boolean = {
    plan match {
      case gbk: Plan.GroupByKeyOp[_, _] =>
        // Can bypass if already partitioned by key with correct partition count
        upstreamPartitioning.exists(p => p.byKey && p.numPartitions == conf.defaultShufflePartitions)

      case rbk: Plan.ReduceByKeyOp[_, _] =>
        // Can bypass if already partitioned by key with correct partition count
        upstreamPartitioning.exists(p => p.byKey && p.numPartitions == conf.defaultShufflePartitions)

      case pby: Plan.PartitionByOp[_, _] =>
        // Can bypass PartitionBy if upstream is already partitioned by key with correct partition count
        // This enables chaining: partitionBy -> groupByKey to be optimized to a single stage
        upstreamPartitioning.exists(p => p.byKey && p.numPartitions == pby.numPartitions)

      case rep: Plan.RepartitionOp[_] =>
        // Can bypass if already has the desired partitioning
        upstreamPartitioning.exists(p => !p.byKey && p.numPartitions == rep.numPartitions)

      case coal: Plan.CoalesceOp[_] =>
        // Coalesce always requires shuffle - cannot bypass even if partition count is already correct
        // because it may need to redistribute data across partitions
        false

      case _ =>
        // Other wide operations cannot bypass shuffle
        false
    }
  }



  /**
   * Creates an Operation from a Plan node.
   * Note: This is a temporary adapter - eventually Plan nodes will be converted to Operations directly.
   */
  private[execution] def fromPlan(plan: Plan[_]): Operation = plan match {
    case Plan.MapOp(_, f) => MapOp(f)
    case Plan.FilterOp(_, p) => FilterOp(p)
    case Plan.FlatMapOp(_, f) => FlatMapOp(f)
    case Plan.DistinctOp(_) => DistinctOp()
    case Plan.KeysOp(_) => KeysOp()
    case Plan.ValuesOp(_) => ValuesOp()
    case Plan.MapValuesOp(_, f) => MapValuesOp(f)
    case Plan.FilterKeysOp(_, p) => FilterKeysOp(p)
    case Plan.FilterValuesOp(_, p) => FilterValuesOp(p)
    case Plan.FlatMapValuesOp(_, f) => FlatMapValuesOp(f)
    case Plan.MapPartitionsOp(_, f) => MapPartitionsOp(f)
    case Plan.GroupByKeyOp(_) => GroupByKeyOp(0) // Will be filled in by builder
    case Plan.ReduceByKeyOp(_, reduceFunc) => ReduceByKeyOp(reduceFunc, 0)
    case Plan.SortByOp(_, keyFunc, _) => SortByOp(keyFunc, 0)
    case Plan.PartitionByOp(_, numPartitions) => PartitionByOp(numPartitions)
    case Plan.RepartitionOp(_, numPartitions) => RepartitionOp(numPartitions)
    case Plan.CoalesceOp(_, numPartitions) => CoalesceOp(numPartitions)
    case Plan.JoinOp(_, _, _) => JoinOp(0)
    case Plan.CoGroupOp(_, _) => CoGroupOp(0)
    case _ => throw new UnsupportedOperationException(s"Cannot convert $plan to Operation")
  }
}
