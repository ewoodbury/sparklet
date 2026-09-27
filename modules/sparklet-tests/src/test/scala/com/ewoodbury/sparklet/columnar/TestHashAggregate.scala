package com.ewoodbury.sparklet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection

class TestHashAggregate extends AnyFlatSpec with Matchers:

  private val sumOnly = HashAggregate.Int32Aggs(sum = true, count = false, min = false, max = false)
  private val allMeasures = HashAggregate.Int32Aggs(sum = true, count = true, min = true, max = true)

  "sum" should "match a grouped sum, including a grow past the initial table" in {
    val rows = (0 until 200).flatMap { key =>
      Seq((key, 1), (key, key), (200 - key, 0))
    }
    sums(HashAggregate.sumInt32(pairs(rows.map { case (k, v) => (Some(k), Some(v)) }), 0, 1)) shouldBe
      oracle(rows)
  }

  it should "match reduceByKey on ints" in {
    val rows = Seq((1, 10), (1, 7), (2, 3), (Int.MinValue, 1), (0, -2), (-1, 5), (2, 4))
    val fromEngine = DistCollection(rows, 4)
      .reduceByKey((left: Int, right: Int) => left + right)
      .collect()
      .map { case (key, value) => key -> value.toLong }
      .toMap
    val summed = sums(HashAggregate.sumInt32(pairs(rows.map { case (k, v) => (Some(k), Some(v)) }), 0, 1))
      .map { case (key, value) => key -> value.getOrElse(fail(s"missing sum for $key")) }

    summed shouldBe fromEngine
  }

  it should "drop null keys and keep a group whose values are all null" in {
    val batch = pairs(
      Seq(
        (Some(1), None),
        (None, Some(9)),
        (Some(1), Some(4)),
        (Some(2), None),
        (None, None),
      ),
    )

    sums(HashAggregate.sumInt32(batch, 0, 1)) shouldBe Map(1 -> Some(4L), 2 -> None)
  }

  it should "not read past the live length" in {
    val keys = Int32Column.of(Array(1, 1, 9), Validity.allValid(3), length = 2)
    val values = Int32Column.of(Array(4, 5, 100), Validity.allValid(3), length = 2)
    val batch = new ColumnBatch(Vector(LogicalType.Int32, LogicalType.Int32), Vector(keys, values), 2)

    sums(HashAggregate.sumInt32(batch, 0, 1)) shouldBe Map(1 -> Some(9L))
  }

  it should "apply the predicate and the map in the aggregate scan" in {
    val rows = Seq((Some(1), Some(5)), (Some(1), Some(-1)), (Some(2), Some(3)), (Some(3), None))
    val fused = HashAggregate.sumInt32(pairs(rows), 0, 1, _ > 0, _ * 10)
    val filtered = ColumnKernel.filterInt32(pairs(rows), 1, _ > 0)
    val projected = ColumnKernel.projectInt32(filtered, 1, _ * 10)
    val staged = HashAggregate.sumInt32(projected, 0, 1)

    sums(fused) shouldBe Map(1 -> Some(50L), 2 -> Some(30L))
    sums(fused) shouldBe sums(staged)
  }

  it should "agree with the multi-measure aggregate on sum" in {
    val rows = Seq((Some(1), Some(3)), (Some(1), Some(1)), (Some(1), Some(8)), (Some(4), Some(-2)))
    val batch = pairs(rows)
    sums(HashAggregate.sumInt32(batch, 0, 1)) shouldBe sums(HashAggregate.aggregateInt32(batch, 0, 1, sumOnly))
  }

  "measures" should "compute sum, count, min, and max together" in {
    val batch = pairs(
      Seq(
        (Some(1), Some(3)),
        (Some(1), None),
        (Some(1), Some(8)),
        (Some(1), Some(1)),
        (Some(2), None),
      ),
    )
    val grouped = HashAggregate.aggregateInt32(batch, 0, 1, allMeasures)

    sums(grouped) shouldBe Map(1 -> Some(12L), 2 -> None)
    longs(grouped, 2) shouldBe Map(1 -> 3L, 2 -> 0L)
    ints(grouped, 3) shouldBe Map(1 -> Some(1), 2 -> None)
    ints(grouped, 4) shouldBe Map(1 -> Some(8), 2 -> None)
    grouped.schema shouldBe Vector(
      LogicalType.Int32,
      LogicalType.Int64,
      LogicalType.Int64,
      LogicalType.Int32,
      LogicalType.Int32,
    )
  }

  it should "sum two max-int values as a long" in {
    val batch = pairs(Seq((Some(1), Some(Int.MaxValue)), (Some(1), Some(Int.MaxValue))))
    sums(HashAggregate.sumInt32(batch, 0, 1)) shouldBe Map(1 -> Some(Int.MaxValue.toLong * 2L))
  }

  it should "return an empty batch for no rows or no present keys" in {
    val empty = HashAggregate.sumInt32(pairs(Seq.empty), 0, 1)
    val dropped = HashAggregate.sumInt32(pairs(Seq((None, Some(1)))), 0, 1)

    empty.length shouldBe 0
    dropped.length shouldBe 0
    empty.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int64)
    sums(empty) shouldBe Map.empty[Int, Option[Long]]
  }

  it should "reject a bad ordinal, a non-int column, or an empty measure set" in {
    val batch = pairs(Seq((Some(1), Some(1))))

    an[IllegalArgumentException] should be thrownBy HashAggregate.sumInt32(batch, 2, 0)
    an[IllegalArgumentException] should be thrownBy {
      HashAggregate.sumInt32(
        new ColumnBatch(
          Vector(LogicalType.Int64, LogicalType.Int32),
          Vector(Int64Column(Seq(1L)), Int32Column(Seq(1))),
          1,
        ),
        0,
        1,
      )
    }
    an[IllegalArgumentException] should be thrownBy {
      HashAggregate.aggregateInt32(
        batch,
        0,
        1,
        HashAggregate.Int32Aggs(sum = false, count = false, min = false, max = false),
      )
    }
  }

  private def pairs(rows: Seq[(Option[Int], Option[Int])]): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.fromNullable(rows.map(_._1)),
        Int32Column.fromNullable(rows.map(_._2)),
      ),
      rows.size,
    )

  private def oracle(rows: Seq[(Int, Int)]): Map[Int, Option[Long]] =
    rows.groupBy(_._1).map { case (key, grouped) =>
      key -> Some(grouped.map(_._2.toLong).sum)
    }

  private def sums(batch: ColumnBatch): Map[Int, Option[Long]] =
    longsOptional(batch, 1)

  private def ints(batch: ColumnBatch, ordinal: Int): Map[Int, Option[Int]] =
    val keys = int32(batch, 0)
    val values = int32(batch, ordinal)
    (0 until batch.length).map { row =>
      val key = keys.values(row)
      val value = if (values.isValid(row)) Some(values.values(row)) else None
      key -> value
    }.toMap

  private def longs(batch: ColumnBatch, ordinal: Int): Map[Int, Long] =
    longsOptional(batch, ordinal).map { case (key, value) =>
      key -> value.getOrElse(fail(s"null long for $key"))
    }

  private def longsOptional(batch: ColumnBatch, ordinal: Int): Map[Int, Option[Long]] =
    val keys = int32(batch, 0)
    val values = int64(batch, ordinal)
    (0 until batch.length).map { row =>
      val key = keys.values(row)
      val value = if (values.isValid(row)) Some(values.values(row)) else None
      key -> value
    }.toMap

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")
