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
