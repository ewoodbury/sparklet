package com.ewoodbury.sparklet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection

class TestHashJoin extends AnyFlatSpec with Matchers:

  "inner join" should "match the row-path join, including duplicate keys" in {
    val buildRows = Seq((1, 10), (1, 11), (2, 20), (4, 1), (Int.MinValue, 7), (0, -1))
    val probeRows = Seq((1, 100), (3, 30), (2, 200), (1, 101), (0, 5), (Int.MinValue, 8))
    val fromEngine = DistCollection(buildRows, 4)
      .join(DistCollection(probeRows, 3))
      .collect()
      .map { case (key, (buildValue, probeValue)) => (key, Some(buildValue), Some(probeValue)) }
      .toSeq
      .sorted

    val joined = HashJoin.innerInt32(
      pairs(buildRows.map { case (key, value) => (Some(key), Some(value)) }),
      0,
      1,
      pairs(probeRows.map { case (key, value) => (Some(key), Some(value)) }),
      0,
      1,
    )

    triples(joined) shouldBe fromEngine
  }

  it should "preserve null values and drop null keys" in {
    val buildRows = Seq((Some(1), Some(10)), (Some(1), None), (None, Some(99)), (Some(2), Some(20)))
    val probeRows = Seq((Some(1), None), (None, Some(7)), (Some(3), Some(3)))

    val expected: Seq[(Int, Option[Int], Option[Int])] = Seq(
      (1, None, None),
      (1, Some(10), None),
    )
    triples(HashJoin.innerInt32(pairs(buildRows), 0, 1, pairs(probeRows), 0, 1)) shouldBe expected.sorted
  }

  it should "return an empty batch when a side is empty or nothing matches" in {
    val build = pairs(Seq((Some(1), Some(1))))
    val probe = pairs(Seq((Some(2), Some(2))))
    val empty = pairs(Seq.empty)

    HashJoin.innerInt32(empty, 0, 1, build, 0, 1).length shouldBe 0
    HashJoin.innerInt32(build, 0, 1, empty, 0, 1).length shouldBe 0
    triples(HashJoin.innerInt32(build, 0, 1, probe, 0, 1)) shouldBe
      Seq.empty[(Int, Option[Int], Option[Int])]
  }

  it should "not read past the live length" in {
    val buildKeys = Int32Column.of(Array(1, 1), Validity.allValid(2), 2)
    val buildValues = Int32Column.of(Array(10, 11), Validity.allValid(2), 2)
    val probeKeys = Int32Column.of(Array(1, 9), Validity.allValid(2), length = 1)
    val probeValues = Int32Column.of(Array(100, 999), Validity.allValid(2), length = 1)
    val build = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(buildKeys, buildValues),
      2,
    )
    val probe = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(probeKeys, probeValues),
      1,
    )

    triples(HashJoin.innerInt32(build, 0, 1, probe, 0, 1)).map(_._2) shouldBe Seq(Some(10), Some(11))
  }

  it should "reject a bad ordinal or a non-int key" in {
    val batch = pairs(Seq((Some(1), Some(1))))

    an[IllegalArgumentException] should be thrownBy HashJoin.innerInt32(batch, 2, 0, batch, 0, 1)
    an[IllegalArgumentException] should be thrownBy {
      HashJoin.innerInt32(
        new ColumnBatch(Vector(LogicalType.Int64), Vector(Int64Column(Seq(1L))), 1),
        0,
        0,
        batch,
        0,
        1,
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

  private def triples(batch: ColumnBatch): Seq[(Int, Option[Int], Option[Int])] =
    val keys = int32(batch, 0)
    val buildValues = int32(batch, 1)
    val probeValues = int32(batch, 2)
    val rows = (0 until batch.length).map { row =>
      val buildValue = if (buildValues.isValid(row)) Some(buildValues.values(row)) else None
      val probeValue = if (probeValues.isValid(row)) Some(probeValues.values(row)) else None
      (keys.values(row), buildValue, probeValue)
    }
    rows.sorted

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")
