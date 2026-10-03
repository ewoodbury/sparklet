package com.ewoodbury.scarlet.columnar

/** Encode Scala sequences into batches, and read those batches back. */
object BatchCodec:

  /** One array fill. `size` then the iterator, so a `Seq` is not copied twice. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def ints(values: Iterable[Int]): ColumnBatch =
    val n = values.size
    val buffer = new Array[Int](n)
    val iterator = values.iterator
    var row = 0
    while (row < n) {
      buffer(row) = iterator.next()
      row += 1
    }
    batch(Int32Column.of(buffer, Validity.allValid(n), n))

  def nullableInts(values: Seq[Option[Int]]): ColumnBatch =
    batch(Int32Column.fromNullable(values))

  def longs(values: Seq[Long]): ColumnBatch =
    batch(Int64Column(values))

  def nullableLongs(values: Seq[Option[Long]]): ColumnBatch =
    batch(Int64Column.fromNullable(values))

  /** One array fill. `size` then the iterator, so a `Seq` is not copied twice. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def float64s(values: Iterable[Double]): ColumnBatch =
    val n = values.size
    val buffer = new Array[Double](n)
    val iterator = values.iterator
    var row = 0
    while (row < n) {
      buffer(row) = iterator.next()
      row += 1
    }
    batch(Float64Column.of(buffer, Validity.allValid(n), n))

  def nullableFloat64s(values: Seq[Option[Double]]): ColumnBatch =
    batch(Float64Column.fromNullable(values))

  def bools(values: Seq[Boolean]): ColumnBatch =
    batch(BoolColumn(values))

  def nullableBools(values: Seq[Option[Boolean]]): ColumnBatch =
    batch(BoolColumn.fromNullable(values))

  def strings(values: Iterable[String]): ColumnBatch =
    batch(DictUtf8Column.fromIterable(values))

  def nullableStrings(values: Seq[Option[String]]): ColumnBatch =
    batch(DictUtf8Column.fromNullable(values))

  /** One fill of each column. `size` then the iterator, so a `Seq` is not copied twice. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def intPairs(values: Iterable[(Int, Int)]): ColumnBatch =
    val n = values.size
    val leftValues = new Array[Int](n)
    val rightValues = new Array[Int](n)
    val iterator = values.iterator
    var row = 0
    while (row < n) {
      val pair = iterator.next()
      leftValues(row) = pair._1
      rightValues(row) = pair._2
      row += 1
    }
    pairBatch(
      Int32Column.of(leftValues, Validity.allValid(n), n),
      Int32Column.of(rightValues, Validity.allValid(n), n),
    )

  def nullableIntPairs(values: Seq[(Option[Int], Option[Int])]): ColumnBatch =
    val n = values.size
    val leftValues = new Array[Int](n)
    val rightValues = new Array[Int](n)
    val leftPresent = new Array[Boolean](n)
    val rightPresent = new Array[Boolean](n)
    values.iterator.zipWithIndex.foreach { (pair, row) =>
      pair._1.foreach { value =>
        leftValues(row) = value
        leftPresent(row) = true
      }
      pair._2.foreach { value =>
        rightValues(row) = value
        rightPresent(row) = true
      }
    }
    pairBatch(
      Int32Column.of(leftValues, Validity.pack(n, row => leftPresent(row)), n),
      Int32Column.of(rightValues, Validity.pack(n, row => rightPresent(row)), n),
    )

  def decodeInts(batch: ColumnBatch): Seq[Int] =
    val column = int32(batch)
    Seq.tabulate(column.length) { row =>
      require(column.isValid(row), s"null Int32 at row $row")
      column.values(row)
    }

  def decodeNullableInts(batch: ColumnBatch): Seq[Option[Int]] =
    val column = int32(batch)
    Seq.tabulate(column.length) { row =>
      if (column.isValid(row)) Some(column.values(row)) else None
    }

  def decodeLongs(batch: ColumnBatch): Seq[Long] =
    val column = int64(batch)
    Seq.tabulate(column.length) { row =>
      require(column.isValid(row), s"null Int64 at row $row")
      column.values(row)
    }

  def decodeNullableLongs(batch: ColumnBatch): Seq[Option[Long]] =
    val column = int64(batch)
    Seq.tabulate(column.length) { row =>
      if (column.isValid(row)) Some(column.values(row)) else None
    }

  def decodeFloat64s(batch: ColumnBatch): Seq[Double] =
    val column = float64(batch)
    Seq.tabulate(column.length) { row =>
      require(column.isValid(row), s"null Float64 at row $row")
      column.values(row)
    }

  def decodeNullableFloat64s(batch: ColumnBatch): Seq[Option[Double]] =
    val column = float64(batch)
    Seq.tabulate(column.length) { row =>
      if (column.isValid(row)) Some(column.values(row)) else None
    }

  def decodeBools(batch: ColumnBatch): Seq[Boolean] =
    val column = bool(batch)
    Seq.tabulate(column.length) { row =>
      require(column.isValid(row), s"null Bool at row $row")
      column.values(row) != 0
    }

  /** An empty cell as a `DistCollection[String]` element. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def stringElement(cell: Option[String]): String =
    cell match
      case Some(text) => text
      case None => null

  /** A `DistCollection[String]` element that is an empty cell. */
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Equals"))
  def isAbsent(text: String): Boolean = text eq null

  def decodeStrings(batch: ColumnBatch): Seq[String] =
    val column = utf8(batch)
    Seq.tabulate(column.length) { row =>
      if (column.isValid(row)) then column.dictionary(column.codes(row))
      else stringElement(None)
    }

  def decodeNullableStrings(batch: ColumnBatch): Seq[Option[String]] =
    val column = utf8(batch)
    Seq.tabulate(column.length) { row =>
      if (column.isValid(row)) Some(column.dictionary(column.codes(row))) else None
    }

  def decodeNullableBools(batch: ColumnBatch): Seq[Option[Boolean]] =
    val column = bool(batch)
    Seq.tabulate(column.length) { row =>
      if (column.isValid(row)) Some(column.values(row) != 0) else None
    }

  def decodeIntPairs(batch: ColumnBatch): Seq[(Int, Int)] =
    val (left, right) = intPair(batch)
    Seq.tabulate(batch.length) { row =>
      require(left.isValid(row) && right.isValid(row), s"null Int32 pair at row $row")
      (left.values(row), right.values(row))
    }

  /**
   * Join rows are `(key, build value, probe value)`. A null stays an error: the decoded `Int`
   * would otherwise be the stored zero. Live bits are checked by word, then values are read
   * unchecked. Spare capacity past `length` is ignored.
   */
  def decodeIntTriples(batch: ColumnBatch): Seq[(Int, (Int, Int))] =
    batch.columns match
      case Seq(keys: Int32Column, build: Int32Column, probe: Int32Column) =>
        require(
          keys.length == batch.length && build.length == batch.length && probe.length == batch.length,
          "join columns disagree on length",
        )
        requireLive(keys)
        requireLive(build)
        requireLive(probe)
        Seq.tabulate(batch.length) { row =>
          (keys.values(row), (build.values(row), probe.values(row)))
        }
      case _ =>
        throw new IllegalArgumentException(
          s"join output is not three Int32 columns: ${batch.schema}",
        )

  def decodeNullableIntPairs(batch: ColumnBatch): Seq[(Option[Int], Option[Int])] =
    val (left, right) = intPair(batch)
    Seq.tabulate(batch.length) { row =>
      val leftValue = if (left.isValid(row)) Some(left.values(row)) else None
      val rightValue = if (right.isValid(row)) Some(right.values(row)) else None
      (leftValue, rightValue)
    }

  /** Throws once, naming the first clear live bit. Bits at `length` and beyond are not live. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def requireLive(column: Int32Column): Unit =
    val live = column.length
    val words = column.validity.words
    val fullWords = live >>> 6
    var index = 0
    while (index < fullWords) {
      if (words(index) != -1L) then failNull(words, index << 6, live)
      index += 1
    }
    val tail = live & 63
    if (tail != 0 && (words(fullWords) & ((1L << tail) - 1L)) != (1L << tail) - 1L) then
      failNull(words, fullWords << 6, live)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def failNull(words: Array[Long], from: Int, live: Int): Nothing =
    var row = from
    val limit = math.min(live, from + 64)
    while (row < limit) {
      if (!Validity.isSet(words, row)) then
        throw new IllegalArgumentException(s"null join value at row $row")
      row += 1
    }
    throw new IllegalArgumentException(s"null join value at row $from")

  private def batch(column: Column): ColumnBatch =
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  private def pairBatch(left: Int32Column, right: Int32Column): ColumnBatch =
    new ColumnBatch(
      schema = Vector(LogicalType.Int32, LogicalType.Int32),
      columns = Vector(left, right),
      length = left.length,
    )

  private def int32(batch: ColumnBatch): Int32Column =
    batch.columns match
      case Seq(column: Int32Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Int32 column, got ${batch.schema}")

  private def int64(batch: ColumnBatch): Int64Column =
    batch.columns match
      case Seq(column: Int64Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Int64 column, got ${batch.schema}")

  private def float64(batch: ColumnBatch): Float64Column =
    batch.columns match
      case Seq(column: Float64Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Float64 column, got ${batch.schema}")

  private def bool(batch: ColumnBatch): BoolColumn =
    batch.columns match
      case Seq(column: BoolColumn) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Bool column, got ${batch.schema}")

  private def utf8(batch: ColumnBatch): DictUtf8Column =
    batch.columns match
      case Seq(column: DictUtf8Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Utf8Dict column, got ${batch.schema}")

  private def intPair(batch: ColumnBatch): (Int32Column, Int32Column) =
    batch.columns match
      case Seq(left: Int32Column, right: Int32Column) => (left, right)
      case _ =>
        throw new IllegalArgumentException(s"expected two Int32 columns, got ${batch.schema}")
