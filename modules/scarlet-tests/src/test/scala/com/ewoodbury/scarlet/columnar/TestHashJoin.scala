package com.ewoodbury.scarlet.columnar

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf

class TestHashJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  "inner join" should "match the row-path join, including duplicate keys" in {
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val buildRows = Seq((1, 10), (1, 11), (2, 20), (4, 1), (Int.MinValue, 7), (0, -1))
    val probeRows = Seq((1, 100), (3, 30), (2, 200), (1, 101), (0, 5), (Int.MinValue, 8))
    val fromEngine = DistCollection(buildRows, 4)
      .join(DistCollection(probeRows, 3))
      .collect()
      .map { case (key, (buildValue, probeValue)) => (key, Some(buildValue), Some(probeValue)) }
      .toSeq
      .sorted

    val joined = join(
      pairs(buildRows.map { case (key, value) => (Some(key), Some(value)) }),
      pairs(probeRows.map { case (key, value) => (Some(key), Some(value)) }),
    )

    sorted(joined) shouldBe fromEngine
  }

  it should "match the row-path join across collisions and duplicate keys" in {
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val buildRows = (0 until 3000).map { row =>
      val key = if (row % 17 == 0) row % 64 else row * 31
      (key, row)
    }
    val probeRows = (0 until 3000).map { row =>
      val key = if (row % 13 == 0) row % 64 else row * 17
      (key, -row)
    }
    val fromEngine = DistCollection(buildRows, 4)
      .join(DistCollection(probeRows, 4))
      .collect()
      .map { case (key, (buildValue, probeValue)) => (key, Some(buildValue), Some(probeValue)) }
      .toSeq
      .sorted

    sorted(
      join(
        pairs(buildRows.map { case (key, value) => (Some(key), Some(value)) }),
        pairs(probeRows.map { case (key, value) => (Some(key), Some(value)) }),
      ),
    ) shouldBe fromEngine
  }

  it should "emit probe order, and newest build rows first within a key" in {
    val buildRows = Seq(
      (Some(1), Some(10)),
      (Some(2), Some(20)),
      (Some(1), Some(11)),
      (Some(1), Some(12)),
    )
    val probeRows = Seq((Some(2), Some(200)), (Some(1), Some(100)), (Some(2), Some(201)))

    inOrder(join(pairs(buildRows), pairs(probeRows))) shouldBe Seq(
      (2, Some(20), Some(200)),
      (1, Some(12), Some(100)),
      (1, Some(11), Some(100)),
      (1, Some(10), Some(100)),
      (2, Some(20), Some(201)),
    )
  }

  it should "preserve null values and drop null keys" in {
    val buildRows = Seq((Some(1), Some(10)), (Some(1), None), (None, Some(99)), (Some(2), Some(20)))
    val probeRows = Seq((Some(1), None), (None, Some(7)), (Some(3), Some(3)))

    val expected: Seq[(Int, Option[Int], Option[Int])] = Seq(
      (1, None, None),
      (1, Some(10), None),
    )
    sorted(join(pairs(buildRows), pairs(probeRows))) shouldBe expected.sorted
  }

  it should "grow the output and keep value validity past a bitmap word" in {
    val buildRows = (0 until 80).map { row =>
      (Some(1), if (row == 9) None else Some(row))
    }
    val joined = join(pairs(buildRows), pairs(Seq((Some(1), Some(7)))))

    joined.length shouldBe 80
    val ordered = inOrder(joined)
    ordered.map(_._3).distinct shouldBe Seq(Some(7))
    ordered.map(_._2) shouldBe (79 to 0 by -1).map { row =>
      if (row == 9) None else Some(row)
    }
  }

  it should "return an empty batch when a side is empty or nothing matches" in {
    val build = pairs(Seq((Some(1), Some(1))))
    val probe = pairs(Seq((Some(2), Some(2))))
    val empty = pairs(Seq.empty)

    join(empty, build).length shouldBe 0
    join(build, empty).length shouldBe 0
    sorted(join(build, probe)) shouldBe Seq.empty[(Int, Option[Int], Option[Int])]
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

    inOrder(join(build, probe)).map(_._2) shouldBe Seq(Some(11), Some(10))
  }

  it should "reject a bad ordinal or a non-int key" in {
    val batch = pairs(Seq((Some(1), Some(1))))

    an[IllegalArgumentException] should be thrownBy {
      HashJoin.innerInt32(
        HashJoin.JoinSide(batch, key = 2, value = 0),
        HashJoin.JoinSide(batch, key = 0, value = 1),
      )
    }
    an[IllegalArgumentException] should be thrownBy {
      HashJoin.innerInt32(
        HashJoin.JoinSide(pairs(Seq.empty), key = 2, value = 0),
        HashJoin.JoinSide(batch, key = 0, value = 1),
      )
    }
    an[IllegalArgumentException] should be thrownBy {
      HashJoin.innerInt32(
        HashJoin.JoinSide(
          new ColumnBatch(Vector(LogicalType.Int64), Vector(Int64Column(Seq(1L))), 1),
          key = 0,
          value = 0,
        ),
        HashJoin.JoinSide(batch, key = 0, value = 1),
      )
    }
  }

  private def join(build: ColumnBatch, probe: ColumnBatch): ColumnBatch =
    HashJoin.innerInt32(
      HashJoin.JoinSide(build, key = 0, value = 1),
      HashJoin.JoinSide(probe, key = 0, value = 1),
    )

  private def pairs(rows: Seq[(Option[Int], Option[Int])]): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.fromNullable(rows.map(_._1)),
        Int32Column.fromNullable(rows.map(_._2)),
      ),
      rows.size,
    )

  private def sorted(batch: ColumnBatch): Seq[(Int, Option[Int], Option[Int])] =
    inOrder(batch).sorted

  private def inOrder(batch: ColumnBatch): Seq[(Int, Option[Int], Option[Int])] =
    val keys = int32(batch, 0)
    val buildValues = int32(batch, 1)
    val probeValues = int32(batch, 2)
    (0 until batch.length).map { row =>
      val buildValue = if (buildValues.isValid(row)) Some(buildValues.values(row)) else None
      val probeValue = if (probeValues.isValid(row)) Some(probeValues.values(row)) else None
      (keys.values(row), buildValue, probeValue)
    }

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")
