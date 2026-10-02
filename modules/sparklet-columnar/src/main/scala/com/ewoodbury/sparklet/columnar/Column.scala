package com.ewoodbury.sparklet.columnar

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
 * Filter, slice, and the hash exchange keep this dictionary. Concat keeps the head dictionary when
 * every piece contains the same strings in the same order, and otherwise builds a new one from the
 * strings that are present.
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
    encode(values.size, values.iterator)

  /**
   * One fill. `size` then the iterator, so a `Seq` is not copied twice. Null elements are empty
   * slots.
   */
  def fromIterable(values: Iterable[String]): DictUtf8Column =
    encode(values.size, values.iterator.map(text => Option(text)))

  private def encode(count: Int, cells: Iterator[Option[String]]): DictUtf8Column =
    val codes = new Array[Int](count)
    val present = new Array[Boolean](count)
    val words = cells.zipWithIndex.foldLeft(Words.empty) { (words, item) =>
      item match
        case (Some(text), row) => assign(words, text, codes, present, row)
        case (None, _) => words
    }
    publish(words, codes, present)

  /** Retains `dictionary` and `codes`. Capacity may exceed `length`. */
  def of(
      dictionary: Array[String],
      codes: Array[Int],
      validity: Validity,
      length: Int,
  ): DictUtf8Column =
    Column.check(length, codes.length, validity)
    new DictUtf8Column(dictionary, codes, validity, length)

  private def assign(
      words: Words,
      text: String,
      codes: Array[Int],
      present: Array[Boolean],
      row: Int,
  ): Words =
    val (next, code) = words.intern(text)
    present(row) = true
    codes(row) = code
    next

  private def publish(words: Words, codes: Array[Int], present: Array[Boolean]): DictUtf8Column =
    of(words.toArray, codes, Validity.pack(present.length, row => present(row)), present.length)

  /** First-seen strings. `entries` is newest-first until [[toArray]]. */
  private[columnar] final case class Words(entries: List[String], index: Map[String, Int]):
    def intern(text: String): (Words, Int) =
      index.get(text) match
        case Some(code) => (this, code)
        case None =>
          val code = index.size
          (Words(text :: entries, index.updated(text, code)), code)

    def toArray: Array[String] = entries.reverse.toArray

  private[columnar] object Words:
    val empty: Words = Words(Nil, Map.empty)
