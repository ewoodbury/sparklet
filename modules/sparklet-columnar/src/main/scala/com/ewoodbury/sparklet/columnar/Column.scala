package com.ewoodbury.sparklet.columnar

import scala.collection.mutable

/**
 * One primitive column. The value array and the validity bitmap share one capacity. `length` is
 * the live row count and may be shorter. Slots at `length` and beyond are undefined.
 *
 * A null slot stores `0` and must not be read as a value. A boolean slot is `0` or `1`. A
 * dictionary string stores an int code, and code `0` is also what a null slot stores, so a reader
 * checks the validity bit before indexing the dictionary. `of` does not scan those conventions:
 * the encoders write them, and a kernel that calls `of` must too.
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

/**
 * Dictionary-coded strings. `dictionary` holds the distinct JVM strings in first-seen order, and
 * `codes` indexes it. There is no separate UTF-8 byte buffer.
 *
 * Filter, slice, and the hash exchange keep this dictionary. Concat keeps it when every piece
 * already shares it, and otherwise builds a new one from the strings that are present.
 */
final class DictUtf8Column private (
    val dictionary: Array[String],
    val codes: Array[Int],
    val validity: Validity,
    val length: Int,
) extends Column:
  def logicalType: LogicalType = LogicalType.Utf8Dict

object DictUtf8Column:

  def apply(values: Seq[String]): DictUtf8Column = fromIterable(values)

  def fromNullable(values: Seq[Option[String]]): DictUtf8Column =
    val codes = new Array[Int](values.size)
    val present = new Array[Boolean](values.size)
    val builder = new Builder
    values.iterator.zipWithIndex.foreach { (cell, row) =>
      cell.foreach(text => accept(text, codes, present, builder, row))
    }
    of(builder.result(), codes, Validity.pack(present.length, row => present(row)), present.length)

  /**
   * One fill. `size` then the iterator, so a `Seq` is not copied twice. A null element is null.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def fromIterable(values: Iterable[String]): DictUtf8Column =
    val n = values.size
    val codes = new Array[Int](n)
    val present = new Array[Boolean](n)
    val builder = new Builder
    val iterator = values.iterator
    var row = 0
    while (row < n) {
      accept(iterator.next(), codes, present, builder, row)
      row += 1
    }
    of(builder.result(), codes, Validity.pack(n, row => present(row)), n)

  /** Retains `dictionary` and `codes`. Capacity may exceed `length`. */
  def of(
      dictionary: Array[String],
      codes: Array[Int],
      validity: Validity,
      length: Int,
  ): DictUtf8Column =
    Column.check(length, codes.length, validity)
    new DictUtf8Column(dictionary, codes, validity, length)

  /** A null element stays a clear bit. Pattern matching does not treat null as a `String`. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def accept(
      text: String,
      codes: Array[Int],
      present: Array[Boolean],
      builder: Builder,
      row: Int,
  ): Unit =
    text match
      case null => ()
      case value =>
        present(row) = true
        codes(row) = builder.code(value)

  /** First-seen dictionary. Not safe to publish until [[result]] is taken. */
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  private[columnar] final class Builder:
    private val words = mutable.ArrayBuffer.empty[String]
    private val index = mutable.HashMap.empty[String, Int]

    def code(text: String): Int =
      index.getOrElseUpdate(
        text, {
          val assigned = words.length
          words += text
          assigned
        },
      )

    def result(): Array[String] = words.toArray
