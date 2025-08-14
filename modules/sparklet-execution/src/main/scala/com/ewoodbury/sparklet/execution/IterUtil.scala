package com.ewoodbury.sparklet.execution

/**
 * Utilities for constructing streaming-backed Iterables.
 */
private[execution] object IterUtil:
  /**
   * Wraps an iterator as a lazy, memoizing Seq using LazyList. This gives us streaming behavior
   * with caching for repeat traversals, and value-based equality with other Seq implementations.
   */
  def iterableOf[A](source: Iterator[A]): Iterable[A] = LazyList.from(source)
