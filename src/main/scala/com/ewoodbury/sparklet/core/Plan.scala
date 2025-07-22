package com.ewoodbury.sparklet.core

/**
 * Represents the logical plan for computing a DistCollection.
 * This is a Directed Acyclic Graph (DAG) where nodes are operations.
 * It's defined recursively.
 *
 * @tparam A The type of elements in the collection represented by this plan node.
 */
sealed trait Plan[A]

object Plan:
  /** The starting point: represents the initial data source.
   * For this local example, it holds an Iterable directly.
   * Using a function `() => Iterable[A]` makes it slightly lazier,
   * ensuring the source data isn't iterated until execution starts.
   */
  case class Source[A](partitions: Seq[Partition[A]]) extends Plan[A]

  /** Represents a map transformation.
   * @param source The preceding plan node (producing elements of type I).
   * @param f The mapping function from I to A.
   * @tparam I The input element type from the source plan.
   */
  case class MapOp[I, A](source: Plan[I], f: I => A) extends Plan[A]

  /** Represents a filter transformation.
   * @param source The preceding plan node (producing elements of type A).
   * @param p The predicate function.
   */
  case class FilterOp[A](source: Plan[A], p: A => Boolean) extends Plan[A]

  /** Represents a flatMap transformation.
   * @param source The preceding plan node (producing elements of type A).
   * @param f The flatMap function from A to IterableOnce[B].
   * @tparam B The type of elements in the resulting DistCollection.
   */
  case class FlatMapOp[A, B](source: Plan[A], f: A => IterableOnce[B]) extends Plan[B]

  /** Represents a distinct transformation.
   * @param source The preceding plan node (producing elements of type A).
   */
  case class DistinctOp[A](source: Plan[A]) extends Plan[A]

  /** Represents a union transformation.
   * @param left The left preceding plan node (producing elements of type A).
   * @param right The right preceding plan node (producing elements of type A).
   */
  case class UnionOp[A](left: Plan[A], right: Plan[A]) extends Plan[A]

  /** Represents a keys transformation.
   * @param source The preceding plan node (producing elements of type (K, V)).
   */
  case class KeysOp[K, V](source: Plan[(K, V)]) extends Plan[K]

  /** Represents a values transformation.
   * @param source The preceding plan node (producing elements of type (K, V)).
   */
  case class ValuesOp[K, V](source: Plan[(K, V)]) extends Plan[V]

  /** Represents a mapValues transformation.
   * @param source The preceding plan node (producing elements of type (K, A)).
   * @param mapFunction The mapping function from A to B.
   * @tparam K The key type.
   * @tparam V The value type of the source plan.
   * @tparam B The type of elements in the resulting DistCollection.
   */
  case class MapValuesOp[K, V, B](source: Plan[(K, V)], mapFunction: V => B) extends Plan[(K, B)]

  /** Represents a filterKeys transformation.
   * @param source The preceding plan node (producing elements of type (K, A)).
   * @param predicateFunction The predicate function from K to Boolean.
   * @tparam K The key type.
   * @tparam V The value type of the source plan.
   */
  case class FilterKeysOp[K, V](source: Plan[(K, V)], predicateFunction: K => Boolean) extends Plan[(K, V)]

  /** Represents a filterValues transformation.
   * @param source The preceding plan node (producing elements of type (K, V)).
   * @param predicateFunction The predicate function from V to Boolean.
   * @tparam K The key type.
   * @tparam V The value type of the source plan.
   */
  case class FilterValuesOp[K, V](source: Plan[(K, V)], predicateFunction: V => Boolean) extends Plan[(K, V)]

  /** Represents a flatMapValues transformation.
   * @param source The preceding plan node (producing elements of type (K, V)).
   * @param flatMapFunction The flatMap function from V to IterableOnce[B].
   * @tparam K The key type.
   * @tparam V The value type of the source plan.
   * @tparam B The type of elements in the resulting DistCollection.
   */
  case class FlatMapValuesOp[K, V, B](source: Plan[(K, V)], flatMapFunction: V => IterableOnce[B]) extends Plan[(K, B)]
end Plan