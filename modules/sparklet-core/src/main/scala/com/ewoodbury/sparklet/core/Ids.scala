package com.ewoodbury.sparklet.core

/**
 * Unique identifier for a stage in an execution graph.
 */
opaque type StageId = Int
object StageId:
  /** Construct a StageId from an `Int`. */
  inline def apply(i: Int): StageId = i

  /** Extract the underlying `Int` for interop. */
  extension (id: StageId) inline def toInt: Int = id
  given Ordering[StageId] with
    def compare(x: StageId, y: StageId): Int = java.lang.Integer.compare(x, y)

/**
 * Unique identifier for a shuffle dataset.
 */
opaque type ShuffleId = Int
object ShuffleId:
  /** Construct a ShuffleId from an `Int`. */
  inline def apply(i: Int): ShuffleId = i

  /** Extract the underlying `Int` for interop. */
  extension (id: ShuffleId) inline def toInt: Int = id
  given Ordering[ShuffleId] with
    def compare(x: ShuffleId, y: ShuffleId): Int = java.lang.Integer.compare(x, y)

/**
 * Partition index within a shuffle dataset.
 */
opaque type PartitionId = Int
object PartitionId:
  /** Construct a PartitionId from an `Int`. */
  inline def apply(i: Int): PartitionId = i

  /** Extract the underlying `Int` for interop. */
  extension (id: PartitionId) inline def toInt: Int = id
  given Ordering[PartitionId] with
    def compare(x: PartitionId, y: PartitionId): Int = java.lang.Integer.compare(x, y)

/**
 * Broadcast dataset identifier for distributed variable sharing.
 */
opaque type BroadcastId = Int
object BroadcastId:
  /** Construct a BroadcastId from an `Int`. */
  inline def apply(i: Int): BroadcastId = i

  /** Extract the underlying `Int` for interop. */
  extension (id: BroadcastId) inline def toInt: Int = id
  given Ordering[BroadcastId] with
    def compare(x: BroadcastId, y: BroadcastId): Int = java.lang.Integer.compare(x, y)
