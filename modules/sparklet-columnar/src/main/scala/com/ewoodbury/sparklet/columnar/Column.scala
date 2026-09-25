package com.ewoodbury.sparklet.columnar

/**
 * One primitive column. The value array and the validity bitmap share one capacity. `length` is
 * the live row count and may be shorter. Slots at `length` and beyond are undefined.
 *
 * A null slot stores `0` and must not be read as a value. A boolean slot is `0` or `1`. `of` does
 * not scan those conventions: the encoders write them, and a kernel that calls `of` must too.
 *
 * The column keeps the arrays it is given. Callers do not mutate them after publication. Kernels
 * allocate a fresh column for their output.
 */
sealed trait Column:
  def logicalType: LogicalType
  def validity: Validity
  def length: Int

  def isValid(row: Int): Boolean =
    require(row >= 0 && row < length, s"row $row outside column length $length")
    validity.isValid(row)

object Column:

  private[columnar] def check(length: Int, valueCapacity: Int, validity: Validity): Unit =
    require(length >= 0, s"length must be >= 0, got $length")
    require(
      valueCapacity >= length,
      s"value capacity $valueCapacity is shorter than length $length",
    )
    require(
      validity.capacity == valueCapacity,
      s"validity capacity ${validity.capacity} does not match value capacity $valueCapacity",
    )

final class Int32Column private (
    val values: Array[Int],
    val validity: Validity,
    val length: Int,
) extends Column:
  def logicalType: LogicalType = LogicalType.Int32

object Int32Column:

  def apply(values: Seq[Int]): Int32Column =
    val buffer = values.toArray
    of(buffer, Validity.allValid(buffer.length), buffer.length)

  def fromNullable(values: Seq[Option[Int]]): Int32Column =
    val buffer = new Array[Int](values.size)
    val present = new Array[Boolean](values.size)
    values.iterator.zipWithIndex.foreach { (cell, row) =>
      cell.foreach { value =>
        buffer(row) = value
        present(row) = true
      }
    }
    of(buffer, Validity.pack(present.length, row => present(row)), present.length)

  /** Retains `values`. Capacity may exceed `length`. */
  def of(values: Array[Int], validity: Validity, length: Int): Int32Column =
    Column.check(length, values.length, validity)
    new Int32Column(values, validity, length)

final class Int64Column private (
    val values: Array[Long],
    val validity: Validity,
    val length: Int,
) extends Column:
  def logicalType: LogicalType = LogicalType.Int64

object Int64Column:

  def apply(values: Seq[Long]): Int64Column =
    val buffer = values.toArray
    of(buffer, Validity.allValid(buffer.length), buffer.length)

  def fromNullable(values: Seq[Option[Long]]): Int64Column =
    val buffer = new Array[Long](values.size)
    val present = new Array[Boolean](values.size)
    values.iterator.zipWithIndex.foreach { (cell, row) =>
      cell.foreach { value =>
        buffer(row) = value
        present(row) = true
      }
    }
    of(buffer, Validity.pack(present.length, row => present(row)), present.length)

  def of(values: Array[Long], validity: Validity, length: Int): Int64Column =
    Column.check(length, values.length, validity)
    new Int64Column(values, validity, length)

final class Float64Column private (
    val values: Array[Double],
    val validity: Validity,
    val length: Int,
) extends Column:
  def logicalType: LogicalType = LogicalType.Float64

object Float64Column:

  def apply(values: Seq[Double]): Float64Column =
    val buffer = values.toArray
    of(buffer, Validity.allValid(buffer.length), buffer.length)

  def fromNullable(values: Seq[Option[Double]]): Float64Column =
    val buffer = new Array[Double](values.size)
    val present = new Array[Boolean](values.size)
    values.iterator.zipWithIndex.foreach { (cell, row) =>
      cell.foreach { value =>
        buffer(row) = value
        present(row) = true
      }
    }
    of(buffer, Validity.pack(present.length, row => present(row)), present.length)

  def of(values: Array[Double], validity: Validity, length: Int): Float64Column =
    Column.check(length, values.length, validity)
    new Float64Column(values, validity, length)

/**
 * Boolean values are bytes, `0` or `1`, not a bitset. A later filter kernel can scan them without
 * unpacking. Nulls still use [[Validity]].
 */
final class BoolColumn private (
    val values: Array[Byte],
    val validity: Validity,
    val length: Int,
) extends Column:
  def logicalType: LogicalType = LogicalType.Bool

object BoolColumn:

  def apply(values: Seq[Boolean]): BoolColumn =
    val buffer = new Array[Byte](values.length)
    values.iterator.zipWithIndex.foreach { (value, row) =>
      buffer(row) = if (value) 1.toByte else 0.toByte
    }
    of(buffer, Validity.allValid(buffer.length), buffer.length)

  def fromNullable(values: Seq[Option[Boolean]]): BoolColumn =
    val buffer = new Array[Byte](values.size)
    val present = new Array[Boolean](values.size)
    values.iterator.zipWithIndex.foreach { (cell, row) =>
      cell.foreach { value =>
        buffer(row) = if (value) 1.toByte else 0.toByte
        present(row) = true
      }
    }
    of(buffer, Validity.pack(present.length, row => present(row)), present.length)

  def of(values: Array[Byte], validity: Validity, length: Int): BoolColumn =
    Column.check(length, values.length, validity)
    new BoolColumn(values, validity, length)
