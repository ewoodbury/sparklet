package com.ewoodbury.scarlet.columnar

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf

class TestHashAggregate extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  private val sumOnly = HashAggregate.Int32Aggs(sum = true, count = false, min = false, max = false)
  private val allMeasures = HashAggregate.Int32Aggs(sum = true, count = true, min = true, max = true)

  "sum" should "match a grouped sum over a few hundred keys" in {
    val rows = (0 until 200).flatMap { key =>
      Seq((key, 1), (key, key), (200 - key, 0))
    }
    sums(HashAggregate.sumInt32(pairs(rows.map { case (k, v) => (Some(k), Some(v)) }), 0, 1)) shouldBe
      oracle(rows)
  }

  it should "match reduceByKey on ints" in {
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
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

  it should "keep an all-null group on the plain path and drop it when filtered" in {
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
    sums(HashAggregate.sumInt32(batch, 0, 1, (_: Int) => true, (value: Int) => value)) shouldBe Map(
      1 -> Some(4L),
    )
    sums(
      HashAggregate.aggregateInt32(batch, 0, 1, sumOnly, (_: Int) => true, (value: Int) => value),
    ) shouldBe Map(1 -> Some(4L))
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
    sums(fused) shouldBe sums(HashAggregate.aggregateInt32(pairs(rows), 0, 1, sumOnly, _ > 0, _ * 10))
  }

  it should "filter a fully dense batch in the sum scan and the measure scan" in {
    val rows = Seq((Some(1), Some(5)), (Some(1), Some(-1)), (Some(2), Some(3)), (Some(4), Some(-2)))
    val batch = pairs(rows)
    val summed = HashAggregate.sumInt32(batch, 0, 1, _ > 0, _ * 10)
    val filtered = ColumnKernel.filterInt32(batch, 1, _ > 0)
    val projected = ColumnKernel.projectInt32(filtered, 1, _ * 10)

    sums(summed) shouldBe Map(1 -> Some(50L), 2 -> Some(30L))
    sums(summed) shouldBe sums(HashAggregate.sumInt32(projected, 0, 1))

    val measured =
      HashAggregate.aggregateInt32(batch, 0, 1, allMeasures, _ > 0, (value: Int) => value)
    sums(measured) shouldBe Map(1 -> Some(5L), 2 -> Some(3L))
    longs(measured, 2) shouldBe Map(1 -> 1L, 2 -> 1L)
    ints(measured, 3) shouldBe Map(1 -> Some(5), 2 -> Some(3))
    ints(measured, 4) shouldBe Map(1 -> Some(5), 2 -> Some(3))
  }

  it should "agree with the multi-measure aggregate on sum" in {
    val rows = Seq((Some(1), Some(3)), (Some(1), Some(1)), (Some(1), Some(8)), (Some(4), Some(-2)))
    val batch = pairs(rows)
    sums(HashAggregate.sumInt32(batch, 0, 1)) shouldBe sums(
      HashAggregate.aggregateInt32(batch, 0, 1, allMeasures),
    )
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

  it should "merge partial sums and partial counts" in {
    val leftRows = Seq((Some(1), Some(10)), (Some(1), None), (Some(2), None), (Some(3), Some(4)))
    val rightRows = Seq((Some(1), Some(1)), (Some(2), Some(5)), (Some(3), Some(6)), (Some(4), Some(7)))
    val left = HashAggregate.sumInt32(pairs(leftRows), 0, 1)
    val right = HashAggregate.sumInt32(pairs(rightRows), 0, 1)
    val merged = HashAggregate.sumInt64(concatMeasure(Seq(left, right), 1), 0, 1)
    val raw = leftRows ++ rightRows

    sums(merged) shouldBe groupedSums(raw)

    val countOnly = HashAggregate.Int32Aggs(sum = false, count = true, min = false, max = false)
    val leftCounts = HashAggregate.aggregateInt32(pairs(leftRows), 0, 1, countOnly)
    val rightCounts = HashAggregate.aggregateInt32(pairs(rightRows), 0, 1, countOnly)
    val mergedCounts = HashAggregate.sumInt64(concatMeasure(Seq(leftCounts, rightCounts), 1), 0, 1)

    longs(mergedCounts, 1) shouldBe groupedCounts(raw)
  }

  it should "rehash every measure once the table passes its load limit" in {
    val rows = (0 until 40).flatMap { key =>
      val edge = key match
        case 0 => Int.MinValue
        case 1 => Int.MaxValue
        case other => other
      Seq((Some(key), Some(edge)), (Some(key), Some(key)))
    } ++ Seq((Some(40), None), (Some(41), Some(5)), (Some(41), None))
    val grouped = HashAggregate.aggregateInt32Rehashing(pairs(rows), 0, 1, allMeasures, 16)

    sums(grouped) shouldBe groupedSums(rows)
    longs(grouped, 2) shouldBe groupedCounts(rows)
    ints(grouped, 3) shouldBe groupedEdges(rows, values => values.foldLeft(Int.MaxValue)(math.min))
    ints(grouped, 4) shouldBe groupedEdges(rows, values => values.foldLeft(Int.MinValue)(math.max))
  }

  it should "size the table to at least twice the row count" in {
    ColumnHash.tableSize(0) shouldBe 16
    ColumnHash.tableSize(8) shouldBe 16
    ColumnHash.tableSize(9) shouldBe 32
    ColumnHash.tableSize(1 << 20) shouldBe (1 << 21)
    ColumnHash.tableSize(1 << 29) shouldBe (1 << 30)
    ColumnHash.tableSize((1 << 29) + 1) shouldBe (1 << 30)
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
    an[IllegalArgumentException] should be thrownBy HashAggregate.sumInt32(pairs(Seq.empty), 2, 0)
    an[IllegalArgumentException] should be thrownBy HashAggregate.sumInt64(batch, 0, 1)
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

  private def groupedSums(rows: Seq[(Option[Int], Option[Int])]): Map[Int, Option[Long]] =
    present(rows).map { case (key, values) =>
      key -> (if (values.isEmpty) None else Some(values.map(_.toLong).sum))
    }

  private def groupedCounts(rows: Seq[(Option[Int], Option[Int])]): Map[Int, Long] =
    present(rows).map { case (key, values) => key -> values.size.toLong }

  private def groupedEdges(
      rows: Seq[(Option[Int], Option[Int])],
      pick: Seq[Int] => Int,
  ): Map[Int, Option[Int]] =
    present(rows).map { case (key, values) =>
      key -> (if (values.isEmpty) None else Some(pick(values)))
    }

  private def present(rows: Seq[(Option[Int], Option[Int])]): Map[Int, Seq[Int]] =
    rows.collect { case (Some(key), value) => key -> value }.groupBy(_._1).map { case (key, grouped) =>
      key -> grouped.flatMap(_._2)
    }

  private def concatMeasure(parts: Seq[ColumnBatch], ordinal: Int): ColumnBatch =
    val rows = parts.flatMap { batch =>
      val keys = int32(batch, 0)
      val values = int64(batch, ordinal)
      (0 until batch.length).map { row =>
        val value = if (values.isValid(row)) Some(values.values(row)) else None
        (Some(keys.values(row)), value)
      }
    }
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64),
      Vector(
        Int32Column.fromNullable(rows.map(_._1)),
        Int64Column.fromNullable(rows.map(_._2)),
      ),
      rows.size,
    )

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
