package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.Plan

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

// Bypassed wide operations, appended as narrow no-op stages when upstream data is already
// partitioned the way the operation wants it
final case class GroupByKeyLocalOp[K, V]() extends Operation[(K, V), (K, Iterable[V])]
final case class ReduceByKeyLocalOp[K, V](reduceFunc: (V, V) => V)
    extends Operation[(K, V), (K, V)]
final case class PartitionByLocalOp[K, V](numPartitions: Int) extends Operation[(K, V), (K, V)]
final case class RepartitionOp[A](numPartitions: Int) extends Operation[A, A]
final case class CoalesceOp[A](numPartitions: Int) extends Operation[A, A]

object Operation {

  /**
   * Determines if a shuffle operation can be bypassed based on upstream partitioning metadata.
   */
  private[execution] def canBypassShuffle(
      plan: Plan[_],
      upstreamPartitioning: Option[StageBuilder.Partitioning],
      conf: com.ewoodbury.sparklet.core.SparkletConf,
  ): Boolean = {
    plan match {
      case _: Plan.GroupByKeyOp[_, _] | _: Plan.ReduceByKeyOp[_, _] =>
        // Can bypass if already partitioned by key with the default shuffle partition count
        upstreamPartitioning.exists(p =>
          p.byKey && p.numPartitions == conf.defaultShufflePartitions,
        )

      case pby: Plan.PartitionByOp[_, _] =>
        // Can bypass if upstream is already partitioned by key with the target partition count
        upstreamPartitioning.exists(p => p.byKey && p.numPartitions == pby.numPartitions)

      case rep: Plan.RepartitionOp[_] =>
        // Can bypass if already has the desired partitioning
        upstreamPartitioning.exists(p => !p.byKey && p.numPartitions == rep.numPartitions)

      case _ =>
        // Coalesce and other wide operations cannot bypass shuffle
        false
    }
  }
}
