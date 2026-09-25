package com.ewoodbury.sparklet.columnar

/**
 * A published batch. `length` is the row count every column shares. The batch does not copy column
 * buffers.
 */
final class ColumnBatch(
    val schema: IndexedSeq[LogicalType],
    val columns: IndexedSeq[Column],
    val length: Int,
):
  require(length >= 0, s"length must be >= 0, got $length")
  require(
    schema.sizeCompare(columns.size) == 0,
    s"schema width ${schema.size} does not match ${columns.size} columns",
  )
  schema.iterator.zip(columns.iterator).zipWithIndex.foreach { (aligned, ordinal) =>
    val (expected, column) = aligned
    require(
      column.logicalType.ordinal == expected.ordinal,
      s"column $ordinal is ${column.logicalType}, schema says $expected",
    )
    require(
      column.length == length,
      s"column $ordinal length ${column.length} does not match batch length $length",
    )
  }
