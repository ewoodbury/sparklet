package com.ewoodbury.sparklet.columnar

/**
 * Multi-batch sum and inner join.
 *
 * Each input batch is sliced into morsels. Sums run `sumInt32` on the morsel, `sumInt64` to merge
 * that batch's partials, and `sumInt64` again after the exchange, so a partial sum does not wrap
 * at `Int`. The join reads key column 0 and value column 1 on both sides, exchanges each side, and
 * probes one partition at a time.
 */
object ColumnPipeline:

  def sumByKey(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(batches.nonEmpty, "pipeline needs at least one batch")
    val combined = PartialCombine.perBatch(
      batches,
      batchSize,
      parallelism,
      morsel => HashAggregate.sumInt32(morsel, keyOrdinal, valueOrdinal),
      partials => HashAggregate.sumInt64(partials, keyOrdinal = 0, valueOrdinal = 1),
    )
    val buckets = HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
    MorselScheduler.map(buckets, parallelism) { (_, bucket) =>
      HashAggregate.sumInt64(bucket, keyOrdinal = 0, valueOrdinal = 1)
    }

  def innerJoin(
      build: Vector[ColumnBatch],
      probe: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    val buildBuckets = HashExchange.partitionAll(
      sliceAll(build, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
    )
    val probeBuckets = HashExchange.partitionAll(
      sliceAll(probe, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
    )
    MorselScheduler.map(probeBuckets, parallelism) { (part, probeBatch) =>
      HashJoin.innerInt32(
        HashJoin.JoinSide(bucketAt(buildBuckets, part), key = 0, value = 1),
        HashJoin.JoinSide(probeBatch, key = 0, value = 1),
      )
    }

  private def bucketAt(buckets: Vector[ColumnBatch], part: Int): ColumnBatch =
    buckets.lift(part).getOrElse {
      throw new IllegalArgumentException(s"missing partition $part")
    }

  private def sliceAll(batches: Vector[ColumnBatch], batchSize: Int): Vector[ColumnBatch] =
    require(batches.nonEmpty, "pipeline needs at least one batch")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    batches.flatMap(batch => ColumnBatches.slice(batch, batchSize))
