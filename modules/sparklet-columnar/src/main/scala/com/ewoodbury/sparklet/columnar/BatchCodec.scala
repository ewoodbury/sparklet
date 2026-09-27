package com.ewoodbury.sparklet.columnar

/** Encode Scala sequences into batches, and read those batches back. */
object BatchCodec:

  def ints(values: Seq[Int]): ColumnBatch =
    val column = Int32Column(values)
    batch(column)

  def nullableInts(values: Seq[Option[Int]]): ColumnBatch =
    batch(Int32Column.fromNullable(values))

  def longs(values: Seq[Long]): ColumnBatch =
    batch(Int64Column(values))

  def nullableLongs(values: Seq[Option[Long]]): ColumnBatch =
    batch(Int64Column.fromNullable(values))

  def float64s(values: Seq[Double]): ColumnBatch =
    batch(Float64Column(values))

  def nullableFloat64s(values: Seq[Option[Double]]): ColumnBatch =
    batch(Float64Column.fromNullable(values))

  def bools(values: Seq[Boolean]): ColumnBatch =
    batch(BoolColumn(values))

  def nullableBools(values: Seq[Option[Boolean]]): ColumnBatch =
    batch(BoolColumn.fromNullable(values))

  def intPairs(values: Seq[(Int, Int)]): ColumnBatch =
    val n = values.size
    val leftValues = new Array[Int](n)
    val rightValues = new Array[Int](n)
    values.iterator.zipWithIndex.foreach { (pair, row) =>
      leftValues(row) = pair._1
      rightValues(row) = pair._2
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

  def decodeNullableIntPairs(batch: ColumnBatch): Seq[(Option[Int], Option[Int])] =
    val (left, right) = intPair(batch)
    Seq.tabulate(batch.length) { row =>
      val leftValue = if (left.isValid(row)) Some(left.values(row)) else None
      val rightValue = if (right.isValid(row)) Some(right.values(row)) else None
      (leftValue, rightValue)
    }

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

  private def intPair(batch: ColumnBatch): (Int32Column, Int32Column) =
    batch.columns match
      case Seq(left: Int32Column, right: Int32Column) => (left, right)
      case _ =>
        throw new IllegalArgumentException(s"expected two Int32 columns, got ${batch.schema}")
