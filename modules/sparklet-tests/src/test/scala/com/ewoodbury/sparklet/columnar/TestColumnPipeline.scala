package com.ewoodbury.sparklet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestColumnPipeline extends AnyFlatSpec with Matchers:

  "sum" should "merge partials across morsels without wrapping at Int" in {
    val batches = Vector(
      BatchCodec.intPairs(Seq((1, 2000000000))),
      BatchCodec.intPairs(Seq((1, 2000000000), (2, 5))),
      BatchCodec.intPairs(Seq((Int.MinValue, -1), (2, 4), (3, 1))),
    )

    val once = sums(runSum(batches, parallelism = 1))
    val parallel = sums(runSum(batches, parallelism = 4))

    once shouldBe Map(
      1 -> Some(4000000000L),
      2 -> Some(9L),
      3 -> Some(1L),
      Int.MinValue -> Some(-1L),
    )
    parallel shouldBe once
  }

  it should "keep a valued partial beside a null partial, and a null sum when every partial is null" in {
    val batches = Vector(
      BatchCodec.nullableIntPairs(Seq((Some(1), Some(7)), (Some(2), None))),
      BatchCodec.nullableIntPairs(Seq((Some(1), None), (Some(2), None), (Some(5), None))),
      BatchCodec.nullableIntPairs(Seq((Some(5), Some(9)), (Some(3), Some(4)))),
    )

    val once = sums(runSum(batches, parallelism = 1), requireValues = false)
    val parallel = sums(runSum(batches, parallelism = 4), requireValues = false)

    once shouldBe Map(1 -> Some(7L), 2 -> None, 5 -> Some(9L), 3 -> Some(4L))
    parallel shouldBe once
  }

  it should "merge a null partial with a valued partial inside one batch" in {
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

    val once = sums(runSum(Vector(batch), parallelism = 1), requireValues = false)
    val parallel = sums(runSum(Vector(batch), parallelism = 4), requireValues = false)

    once shouldBe Map(1 -> Some(7L), 2 -> Some(1L), 3 -> None)
    parallel shouldBe once
  }

  it should "keep an output bucket per partition, including empty ones" in {
    val batches = Vector(BatchCodec.intPairs(Seq((1, 4), (1, 1))))
    val parts = runSum(batches, parallelism = 2)

    parts.length shouldBe 4
    parts.map(_.length).sum shouldBe 1
    sums(parts) shouldBe Map(1 -> Some(5L))
  }

  "inner join" should "match one batch join after the exchange" in {
    val build = Vector(
      BatchCodec.intPairs(Seq((1, 10), (1, 11), (4, 40))),
      BatchCodec.intPairs(Seq((2, 20), (Int.MinValue, 7))),
    )
    val probe = Vector(
      BatchCodec.intPairs(Seq((1, 100), (3, 30))),
      BatchCodec.intPairs(Seq((2, 200), (1, 101), (Int.MinValue, 8))),
    )
    val combinedBuild = ColumnBatches.concat(build)
    val combinedProbe = ColumnBatches.concat(probe)
    val single = HashJoin.innerInt32(
      HashJoin.JoinSide(combinedBuild, key = 0, value = 1),
      HashJoin.JoinSide(combinedProbe, key = 0, value = 1),
    )
    val parts = ColumnPipeline.innerJoin(
      build,
      probe,
      numPartitions = 4,
      parallelism = 4,
      batchSize = 2,
    )

    parts.length shouldBe 4
    triples(parts).sorted shouldBe triples(Vector(single)).sorted
    parts.zipWithIndex.foreach { (part, bucket) =>
      int32(part, 0).values.take(part.length).foreach { key =>
        Math.floorMod(key, 4) shouldBe bucket
      }
    }
  }

  it should "emit an empty partition when one side is empty" in {
    val build = Vector(BatchCodec.intPairs(Seq((1, 10))))
    val probe = Vector(BatchCodec.intPairs(Seq.empty[(Int, Int)]))
    val parts = ColumnPipeline.innerJoin(
      build,
      probe,
      numPartitions = 4,
      parallelism = 2,
      batchSize = 8,
    )

    parts.length shouldBe 4
    parts.map(_.length) shouldBe Seq(0, 0, 0, 0)
  }

  private def runSum(batches: Vector[ColumnBatch], parallelism: Int): Vector[ColumnBatch] =
    ColumnPipeline.sumByKey(
      batches,
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = 4,
      parallelism = parallelism,
      batchSize = 2,
    )

  private def sums(
      batches: Vector[ColumnBatch],
      requireValues: Boolean = true,
  ): Map[Int, Option[Long]] =
    batches.flatMap { batch =>
      val keys = int32(batch, 0)
      val values = int64(batch, 1)
      (0 until batch.length).map { row =>
        require(keys.isValid(row))
        if (requireValues) then require(values.isValid(row))
        val value = if (values.isValid(row)) Some(values.values(row)) else None
        keys.values(row) -> value
      }
    }.toMap

  private def triples(batches: Vector[ColumnBatch]): Seq[(Int, Int, Int)] =
    batches.flatMap { batch =>
      val keys = int32(batch, 0)
      val build = int32(batch, 1)
      val probe = int32(batch, 2)
      (0 until batch.length).map { row =>
        (keys.values(row), build.values(row), probe.values(row))
      }
    }

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")
