package com.ewoodbury.sparklet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestColumnBatch extends AnyFlatSpec with Matchers:

  "primitive batches" should "roundtrip ints, longs, floats, bools, and int pairs" in {
    BatchCodec.decodeInts(BatchCodec.ints(Seq(1, -2, 0))) shouldBe Seq(1, -2, 0)
    BatchCodec.decodeLongs(BatchCodec.longs(Seq(1L, Long.MinValue))) shouldBe Seq(1L, Long.MinValue)
    BatchCodec.decodeFloat64s(BatchCodec.float64s(Seq(1.5, -0.25))) shouldBe Seq(1.5, -0.25)
    BatchCodec.decodeBools(BatchCodec.bools(Seq(true, false, true))) shouldBe Seq(true, false, true)
    BatchCodec.decodeIntPairs(BatchCodec.intPairs(Seq((1, 2), (3, 4)))) shouldBe Seq((1, 2), (3, 4))
  }

  it should "roundtrip empty sequences" in {
    BatchCodec.decodeInts(BatchCodec.ints(Seq.empty[Int])) shouldBe Seq.empty[Int]
    BatchCodec.decodeLongs(BatchCodec.longs(Seq.empty[Long])) shouldBe Seq.empty[Long]
    BatchCodec.decodeIntPairs(BatchCodec.intPairs(Seq.empty[(Int, Int)])) shouldBe
      Seq.empty[(Int, Int)]
  }

  "nulls" should "roundtrip through the validity bitmap" in {
    val ints = Seq(Some(3), None, Some(0), None)
    val longs = Seq(None, Some(8L))
    val floats = Seq(Some(1.0), None)
    val bools = Seq(None, Some(false), Some(true))
    val pairs = Seq((Some(1), None), (None, Some(2)), (None, None))

    BatchCodec.decodeNullableInts(BatchCodec.nullableInts(ints)) shouldBe ints
    BatchCodec.decodeNullableLongs(BatchCodec.nullableLongs(longs)) shouldBe longs
    BatchCodec.decodeNullableFloat64s(BatchCodec.nullableFloat64s(floats)) shouldBe floats
    BatchCodec.decodeNullableBools(BatchCodec.nullableBools(bools)) shouldBe bools
    BatchCodec.decodeNullableIntPairs(BatchCodec.nullableIntPairs(pairs)) shouldBe pairs
  }

  it should "decode join triples and reject a null without turning it into zero" in {
    val live = 70
    val keys = Int32Column.of(Array.tabulate(live + 4)(identity), Validity.allValid(live + 4), live)
    val build = Int32Column(0 until live)
    val probe = Int32Column(0 until live)
    val dense = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(keys, build, probe),
      live,
    )
    val decoded = BatchCodec.decodeIntTriples(dense)
    decoded.length shouldBe live
    decoded.lift(65).getOrElse(fail("missing row 65")) shouldBe ((65, (65, 65)))

    val nullBuild = Int32Column.of(
      Array.tabulate(live)(identity),
      Validity.pack(live, row => row != 65),
      live,
    )
    val sparse = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(Int32Column(0 until live), nullBuild, Int32Column(0 until live)),
      live,
    )
    val thrown = intercept[IllegalArgumentException] {
      BatchCodec.decodeIntTriples(sparse)
    }
    thrown.getMessage shouldBe "null join value at row 65"
  }

  it should "store zero in a null slot and reject a non-null decode" in {
    val column = Int32Column.fromNullable(Seq(Some(4), None))
    column.isValid(0) shouldBe true
    column.isValid(1) shouldBe false
    column.values(1) shouldBe 0

    an[IllegalArgumentException] should be thrownBy {
      BatchCodec.decodeInts(BatchCodec.nullableInts(Seq(Some(4), None)))
    }
  }

  it should "keep presence bits across a 64-row word boundary" in {
    val rows = (0 until 70).map { row =>
      if (row == 63 || row == 64) None else Some(row)
    }

    val decoded = BatchCodec.decodeNullableInts(BatchCodec.nullableInts(rows))

    decoded shouldBe rows
    decoded.slice(62, 66) shouldBe Seq(Some(62), None, None, Some(65))
  }

  "length" should "ignore slots past the live row count" in {
    val values = Array(1, 2, 9, 9)
    val column = Int32Column.of(values, Validity.allValid(values.length), length = 2)
    val batch = new ColumnBatch(Vector(LogicalType.Int32), Vector(column), length = 2)

    column.length shouldBe 2
    values.length shouldBe 4
    BatchCodec.decodeInts(batch) shouldBe Seq(1, 2)
    an[IllegalArgumentException] should be thrownBy column.isValid(2)
  }

  it should "reject a validity bitmap whose capacity differs from the value array" in {
    an[IllegalArgumentException] should be thrownBy {
      Int32Column.of(Array(1, 2, 3), Validity.allValid(1), length = 1)
    }
  }

  "strings" should "collapse duplicates and roundtrip nulls, including code 0" in {
    val rows = Seq(Some("a"), None, Some("a"), Some(""))
    val batch = BatchCodec.nullableStrings(rows)
    val column = utf8(batch)

    BatchCodec.decodeNullableStrings(batch) shouldBe rows
    BatchCodec.decodeStrings(batch) shouldBe rows.map(BatchCodec.stringElement)
    column.dictionary.toSeq shouldBe Seq("a", "")
    column.codes(0) shouldBe 0
    column.isValid(1) shouldBe false
    column.codes(1) shouldBe 0
    column.isValid(3) shouldBe true
    column.codes(3) shouldBe 1
  }

  it should "keep presence bits across a 64-row word boundary" in {
    val rows = (0 until 70).map { row =>
      if (row == 63 || row == 64) None else Some(if (row % 3 == 0) "a" else s"k$row")
    }

    BatchCodec.decodeNullableStrings(BatchCodec.nullableStrings(rows)) shouldBe rows
  }

  it should "share the dictionary across a slice and rebuild it when batches differ" in {
    val values = (0 until 5).map(row => if (row % 2 == 0) "a" else "b")
    val batch = BatchCodec.strings(values)
    val column = utf8(batch)
    val sliced = ColumnBatches.slice(batch, batchSize = 2)
    val joined = ColumnBatches.concat(sliced)

    sliced.length shouldBe 3
    sliced.foreach { part =>
      utf8(part).dictionary should be theSameInstanceAs column.dictionary
    }
    BatchCodec.decodeStrings(joined) shouldBe values
    utf8(joined).dictionary should be theSameInstanceAs column.dictionary

    val left = BatchCodec.strings(Seq("b", "a"))
    val right = BatchCodec.strings(Seq("a", "c"))
    val merged = ColumnBatches.concat(Vector(left, right))

    BatchCodec.decodeStrings(merged) shouldBe Seq("b", "a", "a", "c")
    utf8(merged).dictionary.toSeq shouldBe Seq("b", "a", "c")

    val copy = BatchCodec.strings(Seq("a", "b"))
    val shared = ColumnBatches.concat(Vector(batch, copy))
    utf8(shared).dictionary should be theSameInstanceAs column.dictionary
    BatchCodec.decodeStrings(shared) shouldBe values ++ Seq("a", "b")
  }

  it should "drop unused dictionary entries when a concat remaps" in {
    val sparse = DictUtf8Column.of(
      Array("a", "b"),
      Array(0, 1),
      Validity.pack(2, row => row == 1),
      length = 2,
    )
    val left = new ColumnBatch(Vector(LogicalType.Utf8Dict), Vector(sparse), sparse.length)
    val right = BatchCodec.strings(Seq("c", "a"))
    val merged = ColumnBatches.concat(Vector(left, right))

    BatchCodec.decodeNullableStrings(merged) shouldBe Seq(None, Some("b"), Some("c"), Some("a"))
    utf8(merged).dictionary.toSeq shouldBe Seq("b", "c", "a")
    utf8(merged).isValid(0) shouldBe false
    utf8(merged).codes(0) shouldBe 0
  }

  it should "ignore slots past the live row count" in {
    val dictionary = Array("a", "z")
    val column = DictUtf8Column.of(dictionary, Array(0, 1, 0), Validity.allValid(3), length = 2)
    val batch = new ColumnBatch(Vector(LogicalType.Utf8Dict), Vector(column), length = 2)

    BatchCodec.decodeStrings(batch) shouldBe Seq("a", "z")
  }

  "batch construction" should "reject a column that does not match the schema or the row count" in {
    val one = Int32Column(Seq(1))
    val two = Int32Column(Seq(1, 2))

    an[IllegalArgumentException] should be thrownBy {
      new ColumnBatch(Vector(LogicalType.Int64), Vector(one), length = 1)
    }
    an[IllegalArgumentException] should be thrownBy {
      new ColumnBatch(Vector(LogicalType.Int32, LogicalType.Int32), Vector(one, two), length = 1)
    }
  }

  private def utf8(batch: ColumnBatch): DictUtf8Column =
    batch.columns.lift(0) match
      case Some(column: DictUtf8Column) => column
      case other => fail(s"expected Utf8Dict, got $other")
