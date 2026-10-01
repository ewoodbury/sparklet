package com.ewoodbury.sparklet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestHashExchange extends AnyFlatSpec with Matchers:

  "slice and concat" should "round-trip nulls across a short piece" in {
    val batch = BatchCodec.nullableInts(Seq(Some(1), None, Some(3), Some(4), None))
    val sliced = ColumnBatches.slice(batch, batchSize = 2)

    sliced.map(_.length) shouldBe Seq(2, 2, 1)
    BatchCodec.decodeNullableInts(ColumnBatches.concat(sliced)) shouldBe Seq(
      Some(1),
      None,
      Some(3),
      Some(4),
      None,
    )
  }

  it should "round-trip nulls across a word boundary and unused capacity" in {
    val live = 200
    val capacity = 256
    val values = Array.tabulate(capacity)(identity)
    // Bits past `live` are clear. A shift that pulls them in turns a present row into null.
    val validity = Validity.pack(capacity, row => row < live && row % 7 != 0)
    val column = Int32Column.of(values, validity, live)
    val batch = new ColumnBatch(Vector(LogicalType.Int32), Vector(column), live)
    val expected = (0 until live).map { row =>
      if (row % 7 == 0) None else Some(row)
    }

    val sliced = ColumnBatches.slice(batch, batchSize = 70)
    BatchCodec.decodeNullableInts(ColumnBatches.concat(sliced)) shouldBe expected

    val short = BatchCodec.nullableInts(expected.take(3))
    val restValues = Array.tabulate(live - 3)(row => row + 3)
    val restValidity = Validity.pack(live - 3, row => (row + 3) % 7 != 0)
    val rest = new ColumnBatch(
      Vector(LogicalType.Int32),
      Vector(Int32Column.of(restValues, restValidity, live - 3)),
      live - 3,
    )
    BatchCodec.decodeNullableInts(ColumnBatches.concat(Vector(short, rest))) shouldBe expected
  }

  it should "keep a batch that already fits" in {
    val batch = BatchCodec.ints(Seq(1, 2, 3))

    val sliced = ColumnBatches.slice(batch, batchSize = 8).headOption.getOrElse(fail("empty slice"))
    sliced should be theSameInstanceAs batch
    ColumnBatches.concat(Vector(batch)) should be theSameInstanceAs batch
  }

  "partition" should "keep input order inside a bucket" in {
    val batch = BatchCodec.intPairs(Seq((1, 10), (2, 20), (3, 30), (1, 11)))
    val parts = HashExchange.partitionInt32(batch, keyOrdinal = 0, numPartitions = 2)

    parts.length shouldBe 2
    pairs(bucket(parts, 1)) shouldBe Seq((1, 10), (3, 30), (1, 11))
    pairs(bucket(parts, 2)) shouldBe Seq((2, 20))
  }

  it should "drop null keys and keep a null value" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq((Some(1), Some(10)), (None, Some(99)), (Some(2), None), (Some(1), Some(12))),
    )
    val parts = HashExchange.partitionInt32(batch, keyOrdinal = 0, numPartitions = 2)

    nullablePairs(bucket(parts, 1)) shouldBe Seq((1, Some(10)), (1, Some(12)))
    nullablePairs(bucket(parts, 2)) shouldBe Seq((2, Option.empty[Int]))
    parts.map(_.length).sum shouldBe 3
  }

  it should "match floorMod for negative keys and a long passenger column" in {
    val keys = Int32Column(Seq(Int.MinValue, -1, -3, 4))
    val values = Int64Column(Seq(1L, 2L, 3L, 4L))
    val magnitudes = Float64Column(Seq(1.5, -2.5, 3.5, 4.5))
    val flags = BoolColumn(Seq(true, false, true, false))
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64, LogicalType.Float64, LogicalType.Bool),
      Vector(keys, values, magnitudes, flags),
      4,
    )
    val width = 4
    val parts = HashExchange.partitionInt32(batch, keyOrdinal = 0, numPartitions = width)
    val found = parts.zipWithIndex.flatMap { (part, bucket) =>
      val keyColumn = int32(part, 0)
      val valueColumn = int64(part, 1)
      val magnitudeColumn = float64(part, 2)
      val flagColumn = bool(part, 3)
      (0 until part.length).map { row =>
        keyColumn.values(row) -> (
          valueColumn.values(row),
          magnitudeColumn.values(row),
          flagColumn.values(row) != 0,
          bucket,
        )
      }
    }.toMap

    parts.length shouldBe width
    found(Int.MinValue) shouldBe (1L, 1.5, true, Math.floorMod(Int.MinValue, width))
    found(-1) shouldBe (2L, -2.5, false, Math.floorMod(-1, width))
    found(-3) shouldBe (3L, 3.5, true, Math.floorMod(-3, width))
    found(4) shouldBe (4L, 4.5, false, Math.floorMod(4, width))
  }

  it should "share the input batch when one partition has no null key" in {
    val batch = BatchCodec.ints(Seq(1, 2, 3))
    val parts = HashExchange.partitionInt32(batch, keyOrdinal = 0, numPartitions = 1)

    parts.length shouldBe 1
    parts.headOption.getOrElse(fail("missing batch")) should be theSameInstanceAs batch
  }

  "partitionAll" should "place one heavy key in a single bucket" in {
    val heavy = BatchCodec.intPairs((1 to 1000).map(row => (7, row)))
    val lights = (0 until 6).toVector.map { row =>
      BatchCodec.intPairs(Seq((row, row)))
    }
    val parts = HashExchange.partitionAll(heavy +: lights, keyOrdinal = 0, numPartitions = 4, parallelism = 4)

    parts.length shouldBe 4
    parts.map(_.length).sum shouldBe 1006
    pairs(bucket(parts, 7)).count { case (key, _) => key == 7 } shouldBe 1000
    parts.zipWithIndex.foreach { (part, bucket) =>
      pairs(part).foreach { case (key, _) => Math.floorMod(key, 4) shouldBe bucket }
    }
  }

  private def bucket(parts: Vector[ColumnBatch], key: Int): ColumnBatch =
    parts.lift(Math.floorMod(key, parts.length)).getOrElse(fail(s"missing bucket for $key"))

  private def pairs(batch: ColumnBatch): Seq[(Int, Int)] =
    val keys = int32(batch, 0)
    val values = int32(batch, 1)
    (0 until batch.length).map { row =>
      require(keys.isValid(row) && values.isValid(row))
      (keys.values(row), values.values(row))
    }

  private def nullablePairs(batch: ColumnBatch): Seq[(Int, Option[Int])] =
    val keys = int32(batch, 0)
    val values = int32(batch, 1)
    (0 until batch.length).map { row =>
      require(keys.isValid(row))
      val value = if (values.isValid(row)) Some(values.values(row)) else None
      (keys.values(row), value)
    }

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")

  private def float64(batch: ColumnBatch, ordinal: Int): Float64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Float64Column) => column
      case other => fail(s"expected Float64 at $ordinal, got $other")

  private def bool(batch: ColumnBatch, ordinal: Int): BoolColumn =
    batch.columns.lift(ordinal) match
      case Some(column: BoolColumn) => column
      case other => fail(s"expected Bool at $ordinal, got $other")
