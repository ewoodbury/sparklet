package com.ewoodbury.sparklet.columnar

/**
 * Mixing and sizing for the open-addressed columnar hash kernels.
 *
 * `tableSize` is a power of two, at least `max(16, rows * 2)`, and at most [[MaxCapacity]]. A
 * table that holds one slot per input row then stays at or under half full, so a single batch does
 * not rehash.
 */
private[columnar] object ColumnHash:

  val MaxCapacity: Int = 1 << 30

  def tableSize(rows: Int): Int =
    val need =
      if (rows > (MaxCapacity >>> 1)) MaxCapacity
      else math.max(16, rows << 1)
    if (need >= MaxCapacity) MaxCapacity
    else 1 << (32 - Integer.numberOfLeadingZeros(need - 1))

  def mix(key: Int): Int =
    val first = (key ^ (key >>> 16)) * 0x7feb352d
    val second = (first ^ (first >>> 15)) * 0x846ca68b
    second ^ (second >>> 16)

  def int32At(batch: ColumnBatch, ordinal: Int): Int32Column =
    asInt32(columnAt(batch, ordinal), ordinal)

  def int64At(batch: ColumnBatch, ordinal: Int): Int64Column =
    asInt64(columnAt(batch, ordinal), ordinal)

  private def columnAt(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse {
      throw new IllegalArgumentException(
        s"ordinal $ordinal outside width ${batch.columns.length}",
      )
    }

  private def asInt32(column: Column, ordinal: Int): Int32Column =
    column match
      case int32: Int32Column => int32
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Int32",
        )

  private def asInt64(column: Column, ordinal: Int): Int64Column =
    column match
      case int64: Int64Column => int64
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Int64",
        )
