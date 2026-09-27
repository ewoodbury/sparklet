package com.ewoodbury.sparklet.columnar

/**
 * Hash aggregate over one batch. Group by an `Int32` key. Null keys are dropped and the key is not
 * read. A null value does not update sum, count, min, or max; the group still exists.
 *
 * `sumInt32` with a predicate and a map applies both in the same scan as the hash update. That is
 * filter, then project, then sum, without writing those intermediate batches.
 *
 * These aggregates are associative, so the same function is the partial aggregate and the merge:
 * feed it raw values, or feed `sum` a column of partial sums. Merging partial counts is `sum` of
 * the count column, not `count`.
 *
 * Output order is the hash-table order, not input order and not sorted. Sums are `Long`, so they
 * do not wrap at `Int` width. The value map itself is still `Int` arithmetic.
 *
 * The table is open-addressed and single-threaded. Loops keep the row index in a `var`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashAggregate:

  /** Which measures to emit, in order: sum, count, min, max, after the key. */
  final case class Int32Aggs(sum: Boolean, count: Boolean, min: Boolean, max: Boolean)

  def sumInt32(batch: ColumnBatch, keyOrdinal: Int, valueOrdinal: Int): ColumnBatch =
    val keys = int32At(batch, keyOrdinal)
    val values = int32At(batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then empty(Vector(LogicalType.Int32, LogicalType.Int64))
    else
      val table = SumTable(keys.length)
      if (fullyPresent(keys) && fullyPresent(values)) then addPresent(table, keys, values)
      else addNullable(table, keys, values)
      table.compact()

  /**
   * Rows whose value is null, or for which `keep` is false, are dropped before the group update.
   * `map` runs only for rows that are kept. Both run inside the aggregation scan.
   */
  def sumInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    val keys = int32At(batch, keyOrdinal)
    val values = int32At(batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then empty(Vector(LogicalType.Int32, LogicalType.Int64))
    else
      val table = SumTable(keys.length)
      if (fullyPresent(keys) && fullyPresent(values)) then
        addPresent(table, keys, values, keep, map)
      else addNullable(table, keys, values, keep, map)
      table.compact()

  def aggregateInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      aggs: Int32Aggs,
  ): ColumnBatch =
    aggregateFiltered(
      batch,
      keyOrdinal,
      valueOrdinal,
      aggs,
      filtered = false,
      keep = KeepAll,
      map = Identity,
    )

  def aggregateInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      aggs: Int32Aggs,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    aggregateFiltered(
      batch,
      keyOrdinal,
      valueOrdinal,
      aggs,
      filtered = true,
      keep = keep,
      map = map,
    )

  private object KeepAll extends ColumnKernel.Int32Predicate:
    def apply(value: Int): Boolean = true

  private object Identity extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value

  private def aggregateFiltered(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      aggs: Int32Aggs,
      filtered: Boolean,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    require(aggs.sum || aggs.count || aggs.min || aggs.max, "aggregate needs at least one measure")
    val keys = int32At(batch, keyOrdinal)
    val values = int32At(batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then empty(schema(aggs))
    else
      val table = MeasureTable(keys.length, aggs)
      scanMeasures(table, keys, values, filtered, keep, map)
      table.compact(aggs)

  private def addPresent(table: SumTable, keys: Int32Column, values: Int32Column): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val rows = keys.length
    var row = 0
    while (row < rows) {
      table.add(keyValues(row), valueValues(row).toLong)
      row += 1
    }

  private def addPresent(
      table: SumTable,
      keys: Int32Column,
      values: Int32Column,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val rows = keys.length
    var row = 0
    while (row < rows) {
      val value = valueValues(row)
      if (keep(value)) then table.add(keyValues(row), map(value).toLong)
      row += 1
    }

  private def addNullable(table: SumTable, keys: Int32Column, values: Int32Column): Unit =
    val keyValues = keys.values
    val keyWords = keys.validity.words
    val valueValues = values.values
    val valueWords = values.validity.words
    val rows = keys.length
    var row = 0
    while (row < rows) {
      if (Validity.isSet(keyWords, row)) then
        val slot = table.ensure(keyValues(row))
        if (Validity.isSet(valueWords, row)) then table.observe(slot, valueValues(row).toLong)
      row += 1
    }

  private def addNullable(
      table: SumTable,
      keys: Int32Column,
      values: Int32Column,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): Unit =
    val keyValues = keys.values
    val keyWords = keys.validity.words
    val valueValues = values.values
    val valueWords = values.validity.words
    val rows = keys.length
    var row = 0
    while (row < rows) {
      if (Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row)) then
        val value = valueValues(row)
        if (keep(value)) then table.add(keyValues(row), map(value).toLong)
      row += 1
    }

  private def scanMeasures(
      table: MeasureTable,
      keys: Int32Column,
      values: Int32Column,
      filtered: Boolean,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): Unit =
    val keyValues = keys.values
    val keyWords = keys.validity.words
    val valueValues = values.values
    val valueWords = values.validity.words
    val keyDense = fullyPresent(keys)
    val valueDense = fullyPresent(values)
    val rows = keys.length
    var row = 0
    while (row < rows) {
      val keyPresent = keyDense || Validity.isSet(keyWords, row)
      if (keyPresent) then
        val valuePresent = valueDense || Validity.isSet(valueWords, row)
        val value = valueValues(row)
        val kept = !filtered || (valuePresent && keep(value))
        if (kept) then
          val slot = table.ensure(keyValues(row))
          if (valuePresent) then table.observe(slot, if (filtered) map(value) else value)
      row += 1
    }

  private final class SumTable(rows: Int):
    private var capacity: Int = tableSize(rows)
    private var mask: Int = capacity - 1
    private var occupied: Int = 0
    private var limit: Int = capacity >>> 1
    private var keys: Array[Int] = new Array[Int](capacity)
    private var state: Array[Byte] = new Array[Byte](capacity)
    private var sums: Array[Long] = new Array[Long](capacity)
    private var seen: Array[Byte] = new Array[Byte](capacity)
    private var slots: Array[Int] = new Array[Int](16)

    def add(key: Int, value: Long): Unit =
      val slot = ensure(key)
      sums(slot) += value
      seen(slot) = 1

    def observe(slot: Int, value: Long): Unit =
      sums(slot) += value
      seen(slot) = 1

    def ensure(key: Int): Int =
      if (occupied >= limit) then grow()
      val slot = locate(keys, state, mask, key)
      if (state(slot) == 0) then
        state(slot) = 1
        keys(slot) = key
        remember(slot)
      slot

    def compact(): ColumnBatch =
      val outKeys = new Array[Int](occupied)
      val outSums = new Array[Long](occupied)
      val words = Validity.allocate(occupied)
      var missing = 0
      var cursor = 0
      while (cursor < occupied) {
        val slot = slots(cursor)
        outKeys(cursor) = keys(slot)
        if (seen(slot) == 0) then missing += 1
        else
          outSums(cursor) = sums(slot)
          Validity.setBit(words, cursor)
        cursor += 1
      }
      val sumValidity =
        if (missing == 0) Validity.allValid(occupied) else Validity.fromWords(words, occupied)
      grouped(outKeys, Int64Column.of(outSums, sumValidity, occupied), occupied)

    private def grow(): Unit =
      val oldKeys = keys
      val oldState = state
      val oldSums = sums
      val oldSeen = seen
      val oldCapacity = capacity
      capacity = capacity << 1
      mask = capacity - 1
      limit = capacity >>> 1
      keys = new Array[Int](capacity)
      state = new Array[Byte](capacity)
      sums = new Array[Long](capacity)
      seen = new Array[Byte](capacity)
      occupied = 0
      var slot = 0
      while (slot < oldCapacity) {
        if (oldState(slot) != 0) then
          val placed = locate(keys, state, mask, oldKeys(slot))
          state(placed) = 1
          keys(placed) = oldKeys(slot)
          sums(placed) = oldSums(slot)
          seen(placed) = oldSeen(slot)
          slots(occupied) = placed
          occupied += 1
        slot += 1
      }

    private def remember(slot: Int): Unit =
      if (occupied == slots.length) then
        val bigger = new Array[Int](slots.length << 1)
        System.arraycopy(slots, 0, bigger, 0, occupied)
        slots = bigger
      slots(occupied) = slot
      occupied += 1

  private final class MeasureTable(rows: Int, aggs: Int32Aggs):
    private var capacity: Int = tableSize(rows)
    private var mask: Int = capacity - 1
    private var occupied: Int = 0
    private var limit: Int = capacity >>> 1
    private var keys: Array[Int] = new Array[Int](capacity)
    private var state: Array[Byte] = new Array[Byte](capacity)
    private var sums: Array[Long] =
      if (aggs.sum) new Array[Long](capacity) else Array.emptyLongArray
    private var counts: Array[Long] =
      if (aggs.count) new Array[Long](capacity) else Array.emptyLongArray
    private var mins: Array[Int] = if (aggs.min) new Array[Int](capacity) else Array.emptyIntArray
    private var maxs: Array[Int] = if (aggs.max) new Array[Int](capacity) else Array.emptyIntArray
    private var seen: Array[Byte] = new Array[Byte](capacity)
    private var slots: Array[Int] = new Array[Int](16)

    def ensure(key: Int): Int =
      if (occupied >= limit) then grow()
      val slot = locate(keys, state, mask, key)
      if (state(slot) == 0) then
        state(slot) = 1
        keys(slot) = key
        remember(slot)
      slot

    def observe(slot: Int, value: Int): Unit =
      if (seen(slot) == 0) then
        seen(slot) = 1
        if (sums.length != 0) then sums(slot) = value.toLong
        if (counts.length != 0) then counts(slot) = 1L
        if (mins.length != 0) then mins(slot) = value
        if (maxs.length != 0) then maxs(slot) = value
      else
        if (sums.length != 0) then sums(slot) += value.toLong
        if (counts.length != 0) then counts(slot) += 1L
        if (mins.length != 0 && value < mins(slot)) then mins(slot) = value
        if (maxs.length != 0 && value > maxs(slot)) then maxs(slot) = value

    def compact(aggs: Int32Aggs): ColumnBatch =
      val outKeys = new Array[Int](occupied)
      val outSums = if (aggs.sum) new Array[Long](occupied) else Array.emptyLongArray
      val outCounts = if (aggs.count) new Array[Long](occupied) else Array.emptyLongArray
      val outMins = if (aggs.min) new Array[Int](occupied) else Array.emptyIntArray
      val outMaxs = if (aggs.max) new Array[Int](occupied) else Array.emptyIntArray
      val words = Validity.allocate(occupied)
      var missing = 0
      var cursor = 0
      while (cursor < occupied) {
        val slot = slots(cursor)
        outKeys(cursor) = keys(slot)
        if (seen(slot) == 0) then missing += 1
        else
          if (outSums.length != 0) then outSums(cursor) = sums(slot)
          if (outCounts.length != 0) then outCounts(cursor) = counts(slot)
          if (outMins.length != 0) then outMins(cursor) = mins(slot)
          if (outMaxs.length != 0) then outMaxs(cursor) = maxs(slot)
          Validity.setBit(words, cursor)
        if (seen(slot) == 0 && outCounts.length != 0) then outCounts(cursor) = 0L
        cursor += 1
      }
      val valueValidity =
        if (missing == 0) Validity.allValid(occupied) else Validity.fromWords(words, occupied)
      val countValidity = Validity.allValid(occupied)
      var types = Vector(LogicalType.Int32)
      var columns = Vector[Column](Int32Column.of(outKeys, Validity.allValid(occupied), occupied))
      if (aggs.sum) then
        types = types :+ LogicalType.Int64
        columns = columns :+ Int64Column.of(outSums, valueValidity, occupied)
      if (aggs.count) then
        types = types :+ LogicalType.Int64
        columns = columns :+ Int64Column.of(outCounts, countValidity, occupied)
      if (aggs.min) then
        types = types :+ LogicalType.Int32
        columns = columns :+ Int32Column.of(outMins, valueValidity, occupied)
      if (aggs.max) then
        types = types :+ LogicalType.Int32
        columns = columns :+ Int32Column.of(outMaxs, valueValidity, occupied)
      new ColumnBatch(types, columns, occupied)

    private def grow(): Unit =
      val oldKeys = keys
      val oldState = state
      val oldSums = sums
      val oldCounts = counts
      val oldMins = mins
      val oldMaxs = maxs
      val oldSeen = seen
      val oldCapacity = capacity
      capacity = capacity << 1
      mask = capacity - 1
      limit = capacity >>> 1
      keys = new Array[Int](capacity)
      state = new Array[Byte](capacity)
      sums = if (oldSums.length != 0) new Array[Long](capacity) else Array.emptyLongArray
      counts = if (oldCounts.length != 0) new Array[Long](capacity) else Array.emptyLongArray
      mins = if (oldMins.length != 0) new Array[Int](capacity) else Array.emptyIntArray
      maxs = if (oldMaxs.length != 0) new Array[Int](capacity) else Array.emptyIntArray
      seen = new Array[Byte](capacity)
      occupied = 0
      var slot = 0
      while (slot < oldCapacity) {
        if (oldState(slot) != 0) then
          val placed = locate(keys, state, mask, oldKeys(slot))
          state(placed) = 1
          keys(placed) = oldKeys(slot)
          if (sums.length != 0) then sums(placed) = oldSums(slot)
          if (counts.length != 0) then counts(placed) = oldCounts(slot)
          if (mins.length != 0) then mins(placed) = oldMins(slot)
          if (maxs.length != 0) then maxs(placed) = oldMaxs(slot)
          seen(placed) = oldSeen(slot)
          slots(occupied) = placed
          occupied += 1
        slot += 1
      }

    private def remember(slot: Int): Unit =
      if (occupied == slots.length) then
        val bigger = new Array[Int](slots.length << 1)
        System.arraycopy(slots, 0, bigger, 0, occupied)
        slots = bigger
      slots(occupied) = slot
      occupied += 1

  private def locate(keys: Array[Int], state: Array[Byte], mask: Int, key: Int): Int =
    var slot = mix(key) & mask
    var found = false
    while (!found) {
      if (state(slot) == 0 || keys(slot) == key) then found = true
      else slot = (slot + 1) & mask
    }
    slot

  private def mix(key: Int): Int =
    var mixed = key
    mixed = (mixed ^ (mixed >>> 16)) * 0x7feb352d
    mixed = (mixed ^ (mixed >>> 15)) * 0x846ca68b
    mixed ^ (mixed >>> 16)

  private def tableSize(rows: Int): Int =
    var capacity = 16
    while (capacity < rows && capacity < (1 << 30)) capacity = capacity << 1
    capacity

  private def fullyPresent(column: Column): Boolean =
    val live = column.length
    if (live != column.validity.capacity) then false
    else
      val words = column.validity.words
      val fullWords = live >>> 6
      var index = 0
      var dense = true
      while (index < fullWords && dense) {
        if (words(index) != -1L) then dense = false
        index += 1
      }
      val tail = live & 63
      if (dense && tail != 0) then words(fullWords) == (1L << tail) - 1L else dense

  private def grouped(keys: Array[Int], sums: Int64Column, rows: Int): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64),
      Vector(Int32Column.of(keys, Validity.allValid(rows), rows), sums),
      rows,
    )

  private def empty(types: Vector[LogicalType]): ColumnBatch =
    val columns = types.map { logicalType =>
      logicalType match
        case LogicalType.Int32 =>
          Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0): Column
        case LogicalType.Int64 =>
          Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0): Column
        case other =>
          throw new IllegalArgumentException(s"aggregate schema cannot contain $other")
    }
    new ColumnBatch(types, columns, 0)

  private def schema(aggs: Int32Aggs): Vector[LogicalType] =
    var types = Vector(LogicalType.Int32)
    if (aggs.sum) then types = types :+ LogicalType.Int64
    if (aggs.count) then types = types :+ LogicalType.Int64
    if (aggs.min) then types = types :+ LogicalType.Int32
    if (aggs.max) then types = types :+ LogicalType.Int32
    types

  private def int32At(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case Some(other) =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Int32",
        )
      case None =>
        throw new IllegalArgumentException(
          s"ordinal $ordinal outside width ${batch.columns.length}",
        )
