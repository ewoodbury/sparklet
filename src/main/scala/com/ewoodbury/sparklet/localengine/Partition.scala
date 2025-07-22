package com.ewoodbury.sparklet.localengine

/**
 * A Partition represents a subset of data that can be processed by a single task.
 * It is a collection of elements of type A.
 *
 * @param data The elements in the partition.
 * @tparam A The type of elements in the partition.
 */
final case class Partition[A](data: Iterable[A])
