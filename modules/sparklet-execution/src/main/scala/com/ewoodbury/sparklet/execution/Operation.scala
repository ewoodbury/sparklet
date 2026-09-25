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
 * Metadata for sort operations with typed key function and ordering.
 */
final case class SortWideOpMeta[A, B](
    kind: WideOpKind = WideOpKind.SortBy,
    numPartitions: Int,
    keyFunc: A => B,
    ordering: Ordering[B],
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
 * Structured description of a wide operation attached to a shuffle stage. This is the execution
 * dispatch record: StageExecutor executes a shuffle stage by pattern-matching on it.
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
   * The single named erasure boundary for Plan → Operation conversion. Stage building works with
   * `Operation[Any, Any]` because element types are erased at the stage level; the returned
   * operation retains its real types, so execution is type-correct even though the compiler cannot
   * verify it here.
   */
  @SuppressWarnings(
    Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Any"),
  )
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
    case _ =>
      throw new IllegalArgumentException(
        s"Plan node $plan is not a narrow operation and cannot become a stage Operation",
      )
  }

  /**
   * Determines if a shuffle operation can be bypassed based on upstream partitioning metadata.
   */
  private[execution] def canBypassShuffle(
      plan: Plan[_],
      upstreamPartitioning: Option[PartitioningInfo],
      conf: com.ewoodbury.sparklet.core.SparkletConf,
  ): Boolean = {
    plan match {
      case _: Plan.GroupByKeyOp[_, _] | _: Plan.ReduceByKeyOp[_, _] =>
        // Already hash-partitioned at the default shuffle width.
        upstreamPartitioning.exists(_.isHashPartitioned(conf.defaultShufflePartitions))

      case pby: Plan.PartitionByOp[_, _] =>
        upstreamPartitioning.exists(_.isHashPartitioned(pby.numPartitions))

      case rep: Plan.RepartitionOp[_] =>
        // Old `byKey = false` with a matching width: unknown, range, or round-robin.
        upstreamPartitioning.exists(_.matchesNonHashWidth(rep.numPartitions))

      case _ =>
        // Coalesce and other wide operations cannot bypass shuffle
        false
    }
  }
}
