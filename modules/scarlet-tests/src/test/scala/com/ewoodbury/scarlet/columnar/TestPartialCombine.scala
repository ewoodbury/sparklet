package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestPartialCombine extends AnyFlatSpec with Matchers:

  private val plus: HashReduce.IntBinOp = (left, right) => left + right
  private val max: HashReduce.IntBinOp = (left, right) => math.max(left, right)

  "perBatch" should "merge morsel partials of one input batch" in {
    val batch = BatchCodec.intPairs(Seq((1, 1), (1, 1), (1, 1), (1, 1)))
    val out = reducePartials(Vector(batch), parallelism = 4)

    val merged = one(out)
    merged.length shouldBe 1
    pairs(out) shouldBe Map(1 -> 4)
  }

  it should "keep input batches separate" in {
    val first = BatchCodec.intPairs(Seq((1, 1), (1, 1), (1, 1), (1, 1)))
    val second = BatchCodec.intPairs(Seq((1, 10), (1, 10), (1, 10), (1, 10)))
    val out = reducePartials(Vector(first, second), parallelism = 2)

    val (left, right) = two(out)
    left.length shouldBe 1
    right.length shouldBe 1
    pairs(Vector(left)) shouldBe Map(1 -> 4)
    pairs(Vector(right)) shouldBe Map(1 -> 40)
  }

  it should "merge a batch whose keys do not overlap inside a morsel" in {
    val rows = (0 until 8).map(row => (row, 1))
    val out = reducePartials(Vector(BatchCodec.intPairs(rows)), parallelism = 4)

    out.length shouldBe 1
    pairs(out) shouldBe rows.toMap
  }

  it should "leave a single morsel as that partial" in {
    val batch = BatchCodec.intPairs(Seq((4, 3), (4, 5)))
    val out = reducePartials(Vector(batch), parallelism = 2, batchSize = 8)

    out.length shouldBe 1
    pairs(out) shouldBe Map(4 -> 8)
  }

  it should "merge a null sum with a valued sum inside one batch" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), Some(7)),
        (Some(2), Some(1)),
        (Some(1), None),
        (Some(2), None),
        (Some(3), None),
        (Some(3), None),
      ),
    )
    val out = PartialCombine.perBatch(
      Vector(batch),
      batchSize = 2,
      parallelism = 4,
      morsel => HashAggregate.sumInt32(morsel, keyOrdinal = 0, valueOrdinal = 1),
      partials => HashAggregate.sumInt64(partials, keyOrdinal = 0, valueOrdinal = 1),
    )

    val merged = one(out)
    merged.length shouldBe 3
    sums(out) shouldBe Map(1 -> Some(7L), 2 -> Some(1L), 3 -> None)
  }

  it should "reject a non-positive batch size" in {
    val thrown = intercept[IllegalArgumentException] {
      PartialCombine.perBatch(Vector.empty, batchSize = 0, parallelism = 1, identity, identity)
    }
    thrown.getMessage should include("batch size")
  }

  "byKey" should "match one hash reduce after merging morsels" in {
    val rows = Seq((1, 1), (1, 2), (2, 3), (1, 4), (2, 5), (3, 6), (1, 7), (3, 8))
    val batch = BatchCodec.intPairs(rows)
    val parallel = HashReduce.byKey(
      Vector(batch),
      numPartitions = 4,
      parallelism = 4,
      batchSize = 2,
      plus,
    )
    val once = HashReduce.byKey(
      Vector(batch),
      numPartitions = 4,
      parallelism = 1,
      batchSize = 2,
      plus,
    )

    pairs(parallel) shouldBe Map(1 -> 14, 2 -> 8, 3 -> 14)
    pairs(once) shouldBe pairs(parallel)
  }

  it should "apply max across morsels of one batch" in {
    val rows = Seq((1, 3), (2, 1), (1, 9), (2, 8), (1, 4))
    val reduced = HashReduce.byKey(
      Vector(BatchCodec.intPairs(rows)),
      numPartitions = 4,
      parallelism = 3,
      batchSize = 2,
      max,
    )

    pairs(reduced) shouldBe Map(1 -> 9, 2 -> 8)
  }

  it should "wrap at Int when the partials meet inside the batch" in {
    val rows = Seq((1, Int.MaxValue), (2, 1), (1, 10))
    val reduced = HashReduce.byKey(
      Vector(BatchCodec.intPairs(rows)),
      numPartitions = 2,
      parallelism = 2,
      batchSize = 1,
      plus,
    )

    pairs(reduced) shouldBe Map(1 -> (Int.MaxValue + 10), 2 -> 1)
  }

  it should "keep an empty output bucket per partition" in {
    val reduced = HashReduce.byKey(
      Vector.empty,
      numPartitions = 4,
      parallelism = 2,
      batchSize = 8,
      plus,
    )

    reduced.length shouldBe 4
    reduced.map(_.length) shouldBe Seq(0, 0, 0, 0)
  }

  private def one(batches: Vector[ColumnBatch]): ColumnBatch = batches match
    case Vector(batch) => batch
    case other => fail(s"expected one batch, got ${other.length}")

  private def two(batches: Vector[ColumnBatch]): (ColumnBatch, ColumnBatch) = batches match
    case Vector(left, right) => (left, right)
    case other => fail(s"expected two batches, got ${other.length}")

  private def reducePartials(
      batches: Vector[ColumnBatch],
      parallelism: Int,
      batchSize: Int = 2,
  ): Vector[ColumnBatch] =
    PartialCombine.perBatch(
      batches,
      batchSize,
      parallelism,
      morsel => HashReduce.int32(morsel, keyOrdinal = 0, valueOrdinal = 1, plus),
      partials => HashReduce.int32(partials, keyOrdinal = 0, valueOrdinal = 1, plus),
    )

  private def pairs(batches: Vector[ColumnBatch]): Map[Int, Int] =
    batches.flatMap(batch => BatchCodec.decodeIntPairs(batch)).toMap

  private def sums(batches: Vector[ColumnBatch]): Map[Int, Option[Long]] =
    batches.flatMap { batch =>
      val keys = int32(batch, 0)
      val values = int64(batch, 1)
      (0 until batch.length).map { row =>
        val value = if (values.isValid(row)) Some(values.values(row)) else None
        keys.values(row) -> value
      }
    }.toMap

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")
