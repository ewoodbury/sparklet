package com.ewoodbury.sparklet.columnar

/**
 * Hash-partitions batches on an `Int32` key.
 *
 * The bucket is `floorMod(key, numPartitions)`, the same rule `HashPartitioner` uses for an `Int`
 * key, because `hashCode` is the value. Null keys are dropped. Null values stay null and move with
 * the row. Order inside a bucket is input order. An empty bucket is an empty batch of the same
 * schema.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashExchange:

  def partitionInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      numPartitions: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val rows = batch.length
    if (numPartitions == 1 && ColumnHash.fullyPresent(keys)) then Vector(batch)
    else
      val dest = new Array[Int](rows)
      val counts = new Array[Int](numPartitions)
      val keyValues = keys.values
      val keyWords = keys.validity.words
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          val part = Math.floorMod(keyValues(row), numPartitions)
          dest(row) = part
          counts(part) += 1
        else dest(row) = -1
        row += 1
      }
      val scattered = batch.columns.map(column => scatter(column, dest, counts)).toVector
      Vector.tabulate(numPartitions) { part =>
        val columns = scattered.map(column => bucketAt(column, part))
        new ColumnBatch(batch.schema, columns, counts(part))
      }

  /**
   * Partition every batch, then concatenate bucket `i` across batches. Bucket order follows
   * `batches`.
   */
  def partitionAll(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(batches.nonEmpty, "partitionAll needs at least one batch")
    val scattered = MorselScheduler.map(batches, parallelism) { (_, batch) =>
      partitionInt32(batch, keyOrdinal, numPartitions)
    }
    Vector.tabulate(numPartitions) { part =>
      ColumnBatches.concat(scattered.map(buckets => bucketAt(buckets, part)))
    }

  private def bucketAt[A](buckets: Vector[A], part: Int): A =
    buckets.lift(part).getOrElse {
      throw new IllegalArgumentException(s"missing partition $part")
    }

  private def scatter(column: Column, dest: Array[Int], counts: Array[Int]): Vector[Column] =
    column match
      case int32: Int32Column => scatterInt32(int32, dest, counts)
      case int64: Int64Column => scatterInt64(int64, dest, counts)
      case float64: Float64Column => scatterFloat64(float64, dest, counts)
      case bool: BoolColumn => scatterBool(bool, dest, counts)
      case utf8: DictUtf8Column => scatterUtf8(utf8, dest, counts)

  private def scatterInt32(
      column: Int32Column,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Int](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.values
    val validity = column.validity.words
    var row = 0
    while (row < dest.length) {
      val part = dest(row)
      if (part >= 0) then
        val at = cursor(part)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      row += 1
    }
    outputs.indices.map { part =>
      Int32Column.of(outputs(part), Validity.fromWords(words(part), counts(part)), counts(part))
    }.toVector

  private def scatterInt64(
      column: Int64Column,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Long](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.values
    val validity = column.validity.words
    var row = 0
    while (row < dest.length) {
      val part = dest(row)
      if (part >= 0) then
        val at = cursor(part)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      row += 1
    }
    outputs.indices.map { part =>
      Int64Column.of(outputs(part), Validity.fromWords(words(part), counts(part)), counts(part))
    }.toVector

  private def scatterFloat64(
      column: Float64Column,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Double](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.values
    val validity = column.validity.words
    var row = 0
    while (row < dest.length) {
      val part = dest(row)
      if (part >= 0) then
        val at = cursor(part)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      row += 1
    }
    outputs.indices.map { part =>
      Float64Column.of(outputs(part), Validity.fromWords(words(part), counts(part)), counts(part))
    }.toVector

  private def scatterBool(
      column: BoolColumn,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Byte](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.values
    val validity = column.validity.words
    var row = 0
    while (row < dest.length) {
      val part = dest(row)
      if (part >= 0) then
        val at = cursor(part)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      row += 1
    }
    outputs.indices.map { part =>
      BoolColumn.of(outputs(part), Validity.fromWords(words(part), counts(part)), counts(part))
    }.toVector

  private def scatterUtf8(
      column: DictUtf8Column,
      dest: Array[Int],
      counts: Array[Int],
  ): Vector[Column] =
    val outputs = counts.map(count => new Array[Int](count))
    val words = counts.map(count => Validity.allocate(count))
    val cursor = new Array[Int](counts.length)
    val values = column.codes
    val validity = column.validity.words
    var row = 0
    while (row < dest.length) {
      val part = dest(row)
      if (part >= 0) then
        val at = cursor(part)
        outputs(part)(at) = values(row)
        if (Validity.isSet(validity, row)) then Validity.setBit(words(part), at)
        cursor(part) = at + 1
      row += 1
    }
    outputs.indices.map { part =>
      DictUtf8Column.of(
        column.dictionary,
        outputs(part),
        Validity.fromWords(words(part), counts(part)),
        counts(part),
      )
    }.toVector
