package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestColumnKernel extends AnyFlatSpec with Matchers:

  private def single(column: Column): ColumnBatch =
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  private def utf8(batch: ColumnBatch): DictUtf8Column = utf8At(batch, 0)

  private def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    at(batch, ordinal) match
      case column: DictUtf8Column => column
      case other => fail(s"expected Utf8Dict, got $other")

  private def at(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse(fail(s"missing column $ordinal"))

  "filter" should "match a Seq predicate on ints, longs, floats, and bools" in {
    val ints = Seq(1, -2, 0, 4)
    val longs = Seq(1L, -2L, 0L)
    val floats = Seq(1.5, -0.25, 0.0)
    val bools = Seq(true, false, true)

    BatchCodec.decodeInts(ColumnKernel.filterInt32(BatchCodec.ints(ints), 0, _ > 0)) shouldBe Seq(
      1,
      4,
    )
    BatchCodec.decodeLongs(ColumnKernel.filterInt64(BatchCodec.longs(longs), 0, _ < 0L)) shouldBe
      Seq(-2L)
    BatchCodec.decodeFloat64s(
      ColumnKernel.filterFloat64(BatchCodec.float64s(floats), 0, _ < 0.0),
    ) shouldBe Seq(-0.25)
    BatchCodec.decodeBools(ColumnKernel.filterBool(BatchCodec.bools(bools), 0, value => value))
      .shouldBe(Seq(true, true))

    val dirty = BoolColumn.of(Array[Byte](2, 0), Validity.allValid(2), 2)
    val normalized = ColumnKernel.filterBool(single(dirty), 0, value => value)
    at(normalized, 0) match
      case column: BoolColumn =>
        column.length shouldBe 1
        column.values(0) shouldBe 1.toByte
      case other => fail(s"expected Bool, got $other")
  }

  it should "drop nulls without calling the predicate, and keep a present zero" in {
    val ints = Seq(Some(0), None, Some(1))
    val longs = Seq(Some(1L), None, Some(Long.MinValue))
    val floats = Seq(Some(Double.NaN), Some(1.0), None)
    val bools = Seq(Some(true), Some(false), None)

    val filteredInts = ColumnKernel.filterInt32(BatchCodec.nullableInts(ints), 0, _ >= 0)
    BatchCodec.decodeNullableInts(filteredInts) shouldBe Seq(Some(0), Some(1))

    BatchCodec.decodeNullableLongs(
      ColumnKernel.filterInt64(
        BatchCodec.nullableLongs(longs),
        0,
        value => {
          if (value == 0L) then throw new IllegalStateException("predicate saw a null slot")
          value < 0L
        },
      ),
    ) shouldBe Seq(Some(Long.MinValue))

    BatchCodec.decodeNullableFloat64s(
      ColumnKernel.filterFloat64(
        BatchCodec.nullableFloat64s(floats),
        0,
        value => value > 0.0,
      ),
    ) shouldBe Seq(Some(1.0))

    BatchCodec.decodeNullableBools(
      ColumnKernel.filterBool(
        BatchCodec.nullableBools(bools),
        0,
        value => value,
      ),
    ) shouldBe Seq(Some(true))
  }

  it should "not call the predicate on a null slot that stores a passing payload" in {
    val values = Array(5)
    val column = Int32Column.of(values, Validity.pack(1, _ => false), 1)
    val batch = single(column)

    val filtered = ColumnKernel.filterInt32(
      batch,
      0,
      value => throw new IllegalStateException(s"predicate saw $value"),
    )

    filtered.length shouldBe 0
    values shouldBe Array(5)
    column.length shouldBe 1
  }

  it should "compact every column in order and store zero in a surviving null" in {
    val left = Array(1, 2, 3)
    val right = Array(9, 42, 8)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(left, Validity.allValid(left.length), left.length),
        Int32Column.of(right, Validity.pack(right.length, row => row != 1), right.length),
      ),
      left.length,
    )

    val filtered = ColumnKernel.filterInt32(batch, 0, _ >= 2)

    BatchCodec.decodeNullableIntPairs(filtered) shouldBe Seq((Some(2), None), (Some(3), Some(8)))
    at(filtered, 1) match
      case column: Int32Column =>
        column.values(0) shouldBe 0
        column.isValid(0) shouldBe false
        column.validity.capacity shouldBe column.values.length
      case other => fail(s"expected Int32, got $other")
  }

  it should "compact longs, floats, and bools beside the predicate column" in {
    val width = 3
    val keys = Int32Column.of(Array(1, 5, -3), Validity.allValid(width), width)
    val longs = Int64Column.of(Array(99L, 8L, 7L), Validity.pack(width, row => row != 0), width)
    val floats = Float64Column.of(Array(1.5, -2.0, 9.0), Validity.allValid(width), width)
    val bools = BoolColumn.of(Array[Byte](0, 2, 1), Validity.allValid(width), width)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64, LogicalType.Float64, LogicalType.Bool),
      Vector(keys, longs, floats, bools),
      width,
    )

    val filtered = ColumnKernel.filterInt32(batch, 0, _ > 0)

    filtered.length shouldBe 2
    at(filtered, 1) match
      case column: Int64Column =>
        column.isValid(0) shouldBe false
        column.values(0) shouldBe 0L
        column.isValid(1) shouldBe true
        column.values(1) shouldBe 8L
      case other => fail(s"expected Int64, got $other")
    at(filtered, 2) match
      case column: Float64Column =>
        column.values(0) shouldBe 1.5
        column.values(1) shouldBe -2.0
      case other => fail(s"expected Float64, got $other")
    at(filtered, 3) match
      case column: BoolColumn =>
        column.values(0) shouldBe 0.toByte
        column.isValid(0) shouldBe true
        column.values(1) shouldBe 1.toByte
        column.isValid(1) shouldBe true
      case other => fail(s"expected Bool, got $other")
  }

  it should "keep nulls that sit past a 64-row boundary" in {
    val rows = (0 until 70).map { row =>
      val left = Some(row)
      val right = if (row == 64 || row == 66) None else Some(1)
      (left, right)
    }

    val filtered = ColumnKernel.filterInt32(
      BatchCodec.nullableIntPairs(rows),
      0,
      value => value % 2 == 0,
    )

    val expected = rows.collect { case (Some(value), right) if value % 2 == 0 => (Some(value), right) }
    BatchCodec.decodeNullableIntPairs(filtered) shouldBe expected
    filtered.length shouldBe expected.length
  }

  it should "ignore slots past the live row count" in {
    val raw = Array(1, -2, 8, 99)
    val column = Int32Column.of(raw, Validity.allValid(raw.length), length = 3)
    val filtered = ColumnKernel.filterInt32(single(column), 0, _ > 0)

    BatchCodec.decodeInts(filtered) shouldBe Seq(1, 8)
    filtered.length shouldBe 2
    at(filtered, 0) match
      case out: Int32Column =>
        out.values.length should be >= out.length
        out.validity.capacity shouldBe out.values.length
      case other => fail(s"expected Int32, got $other")
  }

  it should "return an empty batch when the input is empty or nothing survives" in {
    val empty = ColumnKernel.filterInt32(BatchCodec.ints(Seq.empty), 0, _ > 0)
    val dropped = ColumnKernel.filterInt32(BatchCodec.ints(Seq(-1, -2)), 0, _ > 0)
    val pairs = ColumnKernel.filterInt32(BatchCodec.intPairs(Seq((1, 2))), 1, _ > 10)

    BatchCodec.decodeInts(empty) shouldBe Seq.empty[Int]
    BatchCodec.decodeInts(dropped) shouldBe Seq.empty[Int]
    empty.schema shouldBe Vector(LogicalType.Int32)
    pairs.length shouldBe 0
    pairs.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int32)
    BatchCodec.decodeIntPairs(pairs) shouldBe Seq.empty[(Int, Int)]
  }

  it should "keep a null string when the predicate says so, and share the dictionary" in {
    val rows = Seq(Some("a"), None, Some("bb"), Some("a"))
    val input = BatchCodec.nullableStrings(rows)
    val kept = ColumnKernel.filterUtf8(
      input,
      0,
      value =>
        Option(value) match
          case None | Some("a") => true
          case Some(_) => false,
    )
    val dropped = ColumnKernel.filterUtf8(
      input,
      0,
      value =>
        Option(value) match
          case Some(text) => text.startsWith("b")
          case None => false,
    )

    BatchCodec.decodeNullableStrings(kept) shouldBe Seq(Some("a"), None, Some("a"))
    BatchCodec.decodeNullableStrings(dropped) shouldBe Seq(Some("bb"))
    utf8(kept).dictionary should be theSameInstanceAs utf8(input).dictionary
    utf8(kept).isValid(1) shouldBe false
    utf8(kept).codes(1) shouldBe 0
  }

  it should "compact a string column beside the predicate and keep its null" in {
    val keys = Int32Column.of(Array(1, 2, 3), Validity.allValid(3), 3)
    val texts = DictUtf8Column.fromNullable(Seq(Some("a"), None, Some("b")))
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Utf8Dict),
      Vector(keys, texts),
      3,
    )

    val filtered = ColumnKernel.filterInt32(batch, 0, _ >= 2)

    filtered.length shouldBe 2
    val column = utf8At(filtered, 1)
    column.dictionary should be theSameInstanceAs texts.dictionary
    column.isValid(0) shouldBe false
    column.codes(0) shouldBe 0
    column.isValid(1) shouldBe true
    column.dictionary(column.codes(1)) shouldBe "b"
  }

  "project" should "map present values and keep nulls as zero" in {
    val ints = Seq(Some(2), None, Some(0))
    val longs = Seq(Some(Long.MinValue), None)
    val floats = Seq(None, Some(1.5))
    val bools = Seq(Some(true), None, Some(false))

    val inputInts = BatchCodec.nullableInts(ints)
    val projectedInts = ColumnKernel.projectInt32(inputInts, 0, _ * 10)
    BatchCodec.decodeNullableInts(projectedInts) shouldBe Seq(Some(20), None, Some(0))
    at(projectedInts, 0) match
      case column: Int32Column =>
        column.values(1) shouldBe 0
        column.isValid(1) shouldBe false
        column.validity should be theSameInstanceAs at(inputInts, 0).validity
      case other => fail(s"expected Int32, got $other")

    BatchCodec.decodeNullableLongs(
      ColumnKernel.projectInt64(BatchCodec.nullableLongs(longs), 0, value => value),
    ) shouldBe longs
    BatchCodec.decodeNullableFloat64s(
      ColumnKernel.projectFloat64(BatchCodec.nullableFloat64s(floats), 0, _ * 2.0),
    ) shouldBe Seq(None, Some(3.0))
    val projectedBools = ColumnKernel.projectBool(BatchCodec.nullableBools(bools), 0, value => !value)
    BatchCodec.decodeNullableBools(projectedBools) shouldBe Seq(Some(false), None, Some(true))
    at(projectedBools, 0) match
      case column: BoolColumn =>
        column.values(0) shouldBe 0.toByte
        column.values(1) shouldBe 0.toByte
        column.values(2) shouldBe 1.toByte
      case other => fail(s"expected Bool, got $other")
  }

  it should "map null strings and build a new dictionary" in {
    val rows = Seq(Some("a"), None, Some("bb"), Some("a"))
    val input = BatchCodec.nullableStrings(rows)
    val projected = ColumnKernel.projectUtf8(
      input,
      0,
      value =>
        Option(value) match
          case None => "n"
          case Some("bb") => BatchCodec.stringElement(None)
          case Some(text) => text.toUpperCase(java.util.Locale.ENGLISH),
    )
    val column = utf8(projected)

    BatchCodec.decodeNullableStrings(projected) shouldBe Seq(Some("A"), Some("n"), None, Some("A"))
    column.dictionary.toSeq shouldBe Seq("A", "n")
    column.dictionary shouldNot be theSameInstanceAs utf8(input).dictionary
    column.isValid(2) shouldBe false
    column.codes(2) shouldBe 0
    column.codes(3) shouldBe column.codes(0)
  }

  it should "not call the function on a null, and should repair a nonzero null slot" in {
    val raw = Array(42, 3)
    val column = Int32Column.of(raw, Validity.pack(2, row => row == 1), 2)
    val projected = ColumnKernel.projectInt32(
      single(column),
      0,
      value =>
        if (value == 42) then throw new IllegalStateException("mapper saw a null slot")
        else value + 1,
    )

    BatchCodec.decodeNullableInts(projected) shouldBe Seq(None, Some(4))
    at(projected, 0) match
      case out: Int32Column => out.values(0) shouldBe 0
      case other => fail(s"expected Int32, got $other")
  }

  it should "share untouched columns and ignore slots past the live length" in {
    val pairs = BatchCodec.intPairs(Seq((1, 10), (2, 20)))
    val projected = ColumnKernel.projectInt32(pairs, 0, _ + 1)
    at(projected, 1) should be theSameInstanceAs at(pairs, 1)
    BatchCodec.decodeIntPairs(projected) shouldBe Seq((2, 10), (3, 20))

    val raw = Array(1, 2, 99)
    val column = Int32Column.of(raw, Validity.allValid(raw.length), length = 2)
    val shortened = ColumnKernel.projectInt32(
      single(column),
      0,
      value => if (value == 99) then throw new IllegalStateException("read past length") else value + 1,
    )
    BatchCodec.decodeInts(shortened) shouldBe Seq(2, 3)
    shortened.length shouldBe 2
  }

  "filter then project" should "match the same functions on a Seq" in {
    val values = Seq(Some(-1), None, Some(3), Some(0), Some(4))
    val batch = ColumnKernel.projectInt32(
      ColumnKernel.filterInt32(BatchCodec.nullableInts(values), 0, _ > 0),
      0,
      _ * 2,
    )

    val expected = values.collect { case Some(value) if value > 0 => Some(value * 2) }
    BatchCodec.decodeNullableInts(batch) shouldBe expected
  }

  "kernels" should "reject a bad ordinal or a column of the wrong type" in {
    val ints = BatchCodec.ints(Seq(1))
    val pairs = BatchCodec.intPairs(Seq((1, 2)))

    an[IllegalArgumentException] should be thrownBy ColumnKernel.filterInt32(ints, 1, _ > 0)
    an[IllegalArgumentException] should be thrownBy ColumnKernel.filterInt32(ints, -1, _ > 0)
    an[IllegalArgumentException] should be thrownBy ColumnKernel.filterInt64(ints, 0, _ > 0L)
    an[IllegalArgumentException] should be thrownBy ColumnKernel.projectFloat64(pairs, 0, _ * 2.0)
    an[IllegalArgumentException] should be thrownBy ColumnKernel.projectBool(ints, 0, value => value)
  }
