package com.ewoodbury.sparklet.columnar

/**
 * Hash reduce over `Int32` keys and `Int32` values.
 *
 * The binary operator is the caller's. It is applied as values are inserted, including when a
 * partial group is merged after the exchange, so it has to be associative and commutative.
 * Arithmetic wraps at `Int`. This is the `DistCollection.reduceByKey` kernel. Grouped sums that
 * must not wrap use [[HashAggregate.sumInt32]] and [[HashAggregate.sumInt64]] instead.
 *
 * Null keys are dropped. A null value is dropped and does not create a group. Output order is
 * first-seen order inside the batch. The table is at least twice the row count, so one batch does
 * not rehash.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashReduce:

  trait IntBinOp:
    def apply(left: Int, right: Int): Int

  def int32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      op: IntBinOp,
  ): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then emptyPairs
    else reduce(keys, values, op)

  /**
   * Partial-reduce each morsel, exchange on the key, then reduce each bucket. An empty input is
   * `numPartitions` empty pair batches.
   */
  def byKey(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: IntBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(emptyPairs)
    else
      val morsels = batches.flatMap(batch => ColumnBatches.slice(batch, batchSize))
      val partials = MorselScheduler.map(morsels, parallelism) { (_, batch) =>
        int32(batch, keyOrdinal = 0, valueOrdinal = 1, op)
      }
      val buckets = HashExchange.partitionAll(partials, keyOrdinal = 0, numPartitions, parallelism)
      MorselScheduler.map(buckets, parallelism) { (_, bucket) =>
        int32(bucket, keyOrdinal = 0, valueOrdinal = 1, op)
      }

  private def reduce(keys: Int32Column, values: Int32Column, op: IntBinOp): ColumnBatch =
    val table = new Table(ColumnHash.tableSize(keys.length), op)
    if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
      table.scanDense(keys, values)
    else table.scanSparse(keys, values)
    table.compact()

  private def emptyPairs: ColumnBatch =
    val keys = Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0)
    val values = Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0)
    new ColumnBatch(Vector(LogicalType.Int32, LogicalType.Int32), Vector(keys, values), 0)

  private final class Table(capacity: Int, op: IntBinOp):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Int] = new Array[Int](capacity)
    private val values: Array[Int] = new Array[Int](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def scanDense(keyColumn: Int32Column, valueColumn: Int32Column): Unit =
      val keyValues = keyColumn.values
      val valueValues = valueColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        add(keyValues(row), valueValues(row))
        row += 1
      }

    def scanSparse(keyColumn: Int32Column, valueColumn: Int32Column): Unit =
      val keyValues = keyColumn.values
      val valueValues = valueColumn.values
      val keyWords = keyColumn.validity.words
      val valueWords = valueColumn.validity.words
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row)) then
          add(keyValues(row), valueValues(row))
        row += 1
      }

    def compact(): ColumnBatch =
      val rows = occupied
      val outKeys = new Array[Int](rows)
      val outValues = new Array[Int](rows)
      var cursor = 0
      while (cursor < rows) {
        val slot = slots(cursor)
        outKeys(cursor) = keys(slot)
        outValues(cursor) = values(slot)
        cursor += 1
      }
      val present = Validity.allValid(rows)
      new ColumnBatch(
        Vector(LogicalType.Int32, LogicalType.Int32),
        Vector(
          Int32Column.of(outKeys, present, rows),
          Int32Column.of(outValues, present, rows),
        ),
        rows,
      )

    private def add(key: Int, value: Int): Unit =
      val slot = locate(key)
      if (state(slot) == 0) then
        if (occupied >= limit) then
          throw new IllegalStateException(s"hash reduce table is full at $occupied groups")
        state(slot) = 1
        keys(slot) = key
        values(slot) = value
        slots(occupied) = slot
        occupied += 1
      else values(slot) = op(values(slot), value)

    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot
