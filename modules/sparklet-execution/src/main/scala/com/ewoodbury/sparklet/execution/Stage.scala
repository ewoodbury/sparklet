package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.Partition

/**
 * Represents a stage: a sequence of narrow transformations that can be executed together. Each
 * stage operates on partitions independently without requiring shuffles.
 */
sealed trait Stage[A, B] {
  def execute(partition: Partition[A]): Partition[B]
}

object Stage {

  /** A stage that applies a single transformation */
  case class SingleOpStage[A, B](operation: Partition[A] => Partition[B]) extends Stage[A, B] {
    def execute(partition: Partition[A]): Partition[B] = operation(partition)
  }

  /** A stage that chains multiple transformations */
  case class ChainedStage[A, B, C](
      first: Stage[A, B],
      second: Stage[B, C],
  ) extends Stage[A, C] {
    def execute(partition: Partition[A]): Partition[C] =
      second.execute(first.execute(partition))
  }

  // Factory methods for common operations
  def map[A, B](f: A => B): Stage[A, B] = {
    SingleOpStage { p =>
      val it = p.data.iterator.map(f)
      Partition(IterUtil.iterableOf(it))
    }
  }

  def filter[A](predicate: A => Boolean): Stage[A, A] = {
    SingleOpStage { p =>
      val it = p.data.iterator.filter(predicate)
      Partition(IterUtil.iterableOf(it))
    }
  }

  def flatMap[A, B](f: A => IterableOnce[B]): Stage[A, B] = {
    SingleOpStage { p =>
      val it = p.data.iterator.flatMap(a => f(a).iterator)
      Partition(IterUtil.iterableOf(it))
    }
  }

  /**
   * Partition-level transform using iterators to avoid unnecessary materialization.
   */
  def mapPartitions[A, B](f: Iterator[A] => Iterator[B]): Stage[A, B] = {
    SingleOpStage { p =>
      val it = f(p.data.iterator)
      Partition(IterUtil.iterableOf(it))
    }
  }

  def distinct[A]: Stage[A, A] = {
    SingleOpStage { p =>
      val it = p.data.iterator.distinct
      // Distinct must be stable over multiple traversals of the same partition view
      Partition(IterUtil.iterableOf(it))
    }
  }

  // Key-value operations
  def keys[K, V]: Stage[(K, V), K] = {
    SingleOpStage { p =>
      val it = p.data.iterator.map(_._1)
      Partition(IterUtil.iterableOf(it))
    }
  }

  def values[K, V]: Stage[(K, V), V] = {
    SingleOpStage { p =>
      val it = p.data.iterator.map(_._2)
      Partition(IterUtil.iterableOf(it))
    }
  }

  def mapValues[K, V, B](f: V => B): Stage[(K, V), (K, B)] = {
    SingleOpStage { p =>
      val it = p.data.iterator.map { case (k, v) => (k, f(v)) }
      Partition(IterUtil.iterableOf(it))
    }
  }

  def filterKeys[K, V](predicate: K => Boolean): Stage[(K, V), (K, V)] = {
    SingleOpStage { p =>
      val it = p.data.iterator.filter { case (k, _) => predicate(k) }
      Partition(IterUtil.iterableOf(it))
    }
  }

  def filterValues[K, V](predicate: V => Boolean): Stage[(K, V), (K, V)] = {
    SingleOpStage { p =>
      val it = p.data.iterator.filter { case (_, v) => predicate(v) }
      Partition(IterUtil.iterableOf(it))
    }
  }

  def flatMapValues[K, V, B](f: V => IterableOnce[B]): Stage[(K, V), (K, B)] = {
    SingleOpStage { p =>
      val it = p.data.iterator.flatMap { case (k, v) => f(v).iterator.map(b => (k, b)) }
      Partition(IterUtil.iterableOf(it))
    }
  }

  /**
   * Local keyed groupBy on a single partition. Used for shuffle-bypass groupByKey and as the
   * per-partition operator of a groupByKey shuffle stage (input is already key-partitioned).
   * `groupMap` is a single pass; value order inside each group follows input order.
   */
  def groupByKeyLocal[K, V]: Stage[(K, V), (K, Iterable[V])] = {
    SingleOpStage { p =>
      Partition(p.data.groupMap(_._1)(_._2).toSeq)
    }
  }

  /**
   * Local keyed reduce on a single partition. Used for shuffle-bypass reduceByKey and as the
   * per-partition operator of a reduceByKey shuffle stage (input is already key-partitioned).
   * `groupMapReduce` is a single pass; a key with one value is returned unchanged.
   */
  def reduceByKeyLocal[K, V](op: (V, V) => V): Stage[(K, V), (K, V)] = {
    SingleOpStage { p =>
      Partition(p.data.groupMapReduce(_._1)(_._2)(op).toSeq)
    }
  }

  /**
   * Local sort of a range-partitioned `(sortKey, element)` partition, emitting elements in sort
   * key order. Used by the sortBy shuffle stage; concatenating these partitions in index order
   * yields a globally ordered result.
   */
  def sortLocal[S, A](ordering: Ordering[S]): Stage[(S, A), A] = {
    SingleOpStage { p =>
      Partition(p.data.toSeq.sortBy(_._1)(ordering).map(_._2))
    }
  }

  /**
   * Identity operation that returns the input partition unchanged. Used for bypassed operations
   * that don't need to transform the data.
   */
  def identity[A]: Stage[A, A] = {
    SingleOpStage { p => p }
  }
}
