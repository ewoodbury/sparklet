package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.Partition

/**
 * Represents a stage: a sequence of narrow transformations that can be executed together. Each
 * stage operates on partitions independently without requiring shuffles.
 */
sealed trait Stage[A, B]:
  def execute(partition: Partition[A]): Partition[B]

object Stage:
  /** A stage that applies a single transformation */
  case class SingleOpStage[A, B](operation: Partition[A] => Partition[B]) extends Stage[A, B]:
    def execute(partition: Partition[A]): Partition[B] = operation(partition)

  /** A stage that chains multiple transformations */
  case class ChainedStage[A, B, C](
      first: Stage[A, B],
      second: Stage[B, C],
  ) extends Stage[A, C]:
    def execute(partition: Partition[A]): Partition[C] =
      second.execute(first.execute(partition))

  // Factory methods for common operations
  def map[A, B](f: A => B): Stage[A, B] =
    SingleOpStage(p => Partition(p.data.map(f)))

  def filter[A](predicate: A => Boolean): Stage[A, A] =
    SingleOpStage(p => Partition(p.data.filter(predicate)))

  def flatMap[A, B](f: A => IterableOnce[B]): Stage[A, B] =
    SingleOpStage(p => Partition(p.data.flatMap(f)))

  def distinct[A]: Stage[A, A] =
    SingleOpStage(p => Partition(p.data.toSeq.distinct))

  // Key-value operations
  def keys[K, V]: Stage[(K, V), K] =
    SingleOpStage(p => Partition(p.data.map(_._1)))

  def values[K, V]: Stage[(K, V), V] =
    SingleOpStage(p => Partition(p.data.map(_._2)))

  def mapValues[K, V, B](f: V => B): Stage[(K, V), (K, B)] =
    SingleOpStage(p => Partition(p.data.map { case (k, v) => (k, f(v)) }))

  def filterKeys[K, V](predicate: K => Boolean): Stage[(K, V), (K, V)] =
    SingleOpStage(p => Partition(p.data.filter { case (k, _) => predicate(k) }))

  def filterValues[K, V](predicate: V => Boolean): Stage[(K, V), (K, V)] =
    SingleOpStage(p => Partition(p.data.filter { case (_, v) => predicate(v) }))

  def flatMapValues[K, V, B](f: V => IterableOnce[B]): Stage[(K, V), (K, B)] =
    SingleOpStage(p => Partition(p.data.flatMap { case (k, v) => f(v).iterator.map(b => (k, b)) }))
