package com.ewoodbury.sparklet.columnar

import java.util.Arrays

/**
 * Hash aggregate over one batch. Group by an `Int32` key. Null keys are dropped and the key is not
 * read.
 *
 * On the plain overloads, a null value does not update a measure and the group still exists. Sum,
 * min, and max stay null until a value is seen. Count stays `0`.
 *
 * The overloads that take `keep` and `map` run filter, then project, then aggregate, in one scan.
 * A null value is dropped before `keep`, and a rejected row never creates a group. A key whose
 * rows are all dropped is absent. A `keep` that accepts every present value is therefore not the
 * plain overload.
 *
 * `sumInt32` reads `Int32` values and writes an `Int64` sum. `sumInt64` reads `Int64` values.
 * Partial sums merge by calling `sumInt64` on the sum column. Partial counts merge by calling
 * `sumInt64` on the count column, not by calling `count` again. Min and max are not merged here.
 *
 * Output order is hash-table order, not input order and not sorted. Sums do not wrap at `Int`
 * width. The value map itself is still `Int` arithmetic.
 *
 * One open-addressed table serves every measure set. Capacity is at least twice the row count, so
 * one batch does not rehash; a shorter capacity exists only so tests can force `grow`. Loops keep
 * the row index in a `var` so the bodies stay allocation-free, matching `ColumnKernel`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashAggregate:

  /**
   * Measures emitted after the key, in this order: sum, count, min, max. Build it with named
   * arguments. The four booleans are easy to swap if they are passed by position.
   */
  final case class Int32Aggs(sum: Boolean, count: Boolean, min: Boolean, max: Boolean)

  /** Keeps a group whose values are all null, and leaves its sum null. */
  def sumInt32(batch: ColumnBatch, keyOrdinal: Int, valueOrdinal: Int): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    sumColumns(
      keys,
      values,
      ColumnHash.tableSize(keys.length),
      filtered = false,
      KeepAll,
      Identity,
    )

  /**
   * Drops a null value before `keep`. A key whose rows are all dropped is absent, unlike the plain
   * `sumInt32`.
   */
  def sumInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    sumColumns(keys, values, ColumnHash.tableSize(keys.length), filtered = true, keep, map)

  /**
   * Sums an `Int64` column into an `Int64` sum. This is the merge for partial sums, and for
   * partial counts when `valueOrdinal` points at the count column. A null value does not update
   * the sum, and the group still exists.
   */
  def sumInt64(batch: ColumnBatch, keyOrdinal: Int, valueOrdinal: Int): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int64At(batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then emptySum
    else
      val table = new GroupTable(ColumnHash.tableSize(keys.length), SumOnly)
      if (fullyPresent(keys) && fullyPresent(values)) then scanSumLongDense(table, keys, values)
      else scanSumLongSparse(table, keys, values)
      table.compact()

  /** Keeps a group whose values are all null. Sum, min, and max are null, and count is 0. */
  def aggregateInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      aggs: Int32Aggs,
  ): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    aggregateColumns(
      keys,
      values,
      aggs,
      filtered = false,
      KeepAll,
      Identity,
      ColumnHash.tableSize(keys.length),
    )

  /**
   * Drops a null value before `keep`. A key whose rows are all dropped is absent, unlike the plain
   * `aggregateInt32`.
   */
  def aggregateInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      aggs: Int32Aggs,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    aggregateColumns(
      keys,
      values,
      aggs,
      filtered = true,
      keep,
      map,
      ColumnHash.tableSize(keys.length),
    )

  /**
   * Same as `aggregateInt32`, but the table starts at `initialCapacity` instead of `tableSize`.
   * Tests pass a short power of two so `grow` runs. Production callers use the row-sized table.
   */
  private[columnar] def aggregateInt32Rehashing(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      aggs: Int32Aggs,
      initialCapacity: Int,
  ): ColumnBatch =
    require(
      Integer.bitCount(initialCapacity) == 1 &&
        initialCapacity >= 16 &&
        initialCapacity <= ColumnHash.MaxCapacity,
      s"initial capacity must be a power of two from 16 to ${ColumnHash.MaxCapacity}",
    )
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    aggregateColumns(keys, values, aggs, filtered = false, KeepAll, Identity, initialCapacity)

  private object KeepAll extends ColumnKernel.Int32Predicate:
    def apply(value: Int): Boolean = true

  private object Identity extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value

  private val SumOnly = Int32Aggs(sum = true, count = false, min = false, max = false)

  private def sumColumns(
      keys: Int32Column,
      values: Int32Column,
      capacity: Int,
      filtered: Boolean,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then emptySum
    else
      val table = new GroupTable(capacity, SumOnly)
      if (fullyPresent(keys) && fullyPresent(values)) then
        if (filtered) then scanSumDenseFiltered(table, keys, values, keep, map)
        else scanSumDense(table, keys, values)
      else scanSumSparse(table, keys, values, filtered, keep, map)
      table.compact()

  private def aggregateColumns(
      keys: Int32Column,
      values: Int32Column,
      aggs: Int32Aggs,
      filtered: Boolean,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
      capacity: Int,
  ): ColumnBatch =
    require(aggs.sum || aggs.count || aggs.min || aggs.max, "aggregate needs at least one measure")
    if (aggs.sum && !aggs.count && !aggs.min && !aggs.max) then
      sumColumns(keys, values, capacity, filtered, keep, map)
    else
      require(keys.length == values.length, "key and value columns differ in length")
      if (keys.length == 0) then empty(schema(aggs))
      else
        val table = new GroupTable(capacity, aggs)
        if (fullyPresent(keys) && fullyPresent(values)) then
          if (filtered) then scanMeasureDenseFiltered(table, keys, values, keep, map)
          else scanMeasureDense(table, keys, values)
        else scanMeasureSparse(table, keys, values, filtered, keep, map)
        table.compact()

  private def scanSumDense(table: GroupTable, keys: Int32Column, values: Int32Column): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val rows = keys.length
    var row = 0
    while (row < rows) {
      table.addSum(keyValues(row), valueValues(row).toLong)
      row += 1
    }

  private def scanSumDenseFiltered(
      table: GroupTable,
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
      if (keep(value)) then table.addSum(keyValues(row), map(value).toLong)
      row += 1
    }

  private def scanSumSparse(
      table: GroupTable,
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
    val rows = keys.length
    var row = 0
    while (row < rows) {
      if (Validity.isSet(keyWords, row)) then
        val valuePresent = Validity.isSet(valueWords, row)
        if (filtered) then
          if (valuePresent) then
            val value = valueValues(row)
            if (keep(value)) then table.addSum(keyValues(row), map(value).toLong)
        else
          val slot = table.ensure(keyValues(row))
          if (valuePresent) then table.accumulate(slot, valueValues(row).toLong)
      row += 1
    }

  private def scanSumLongDense(table: GroupTable, keys: Int32Column, values: Int64Column): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val rows = keys.length
    var row = 0
    while (row < rows) {
      table.addSum(keyValues(row), valueValues(row))
      row += 1
    }

  private def scanSumLongSparse(table: GroupTable, keys: Int32Column, values: Int64Column): Unit =
    val keyValues = keys.values
    val keyWords = keys.validity.words
    val valueValues = values.values
    val valueWords = values.validity.words
    val rows = keys.length
    var row = 0
    while (row < rows) {
      if (Validity.isSet(keyWords, row)) then
        val slot = table.ensure(keyValues(row))
        if (Validity.isSet(valueWords, row)) then table.accumulate(slot, valueValues(row))
      row += 1
    }

  private def scanMeasureDense(table: GroupTable, keys: Int32Column, values: Int32Column): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val rows = keys.length
    var row = 0
    while (row < rows) {
      table.observe(table.ensure(keyValues(row)), valueValues(row))
      row += 1
    }

  private def scanMeasureDenseFiltered(
      table: GroupTable,
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
      if (keep(value)) then table.observe(table.ensure(keyValues(row)), map(value))
      row += 1
    }

  private def scanMeasureSparse(
      table: GroupTable,
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
    val rows = keys.length
    var row = 0
    while (row < rows) {
      if (Validity.isSet(keyWords, row)) then
        val valuePresent = Validity.isSet(valueWords, row)
        if (filtered) then
          if (valuePresent) then
            val value = valueValues(row)
            if (keep(value)) then table.observe(table.ensure(keyValues(row)), map(value))
        else
          val slot = table.ensure(keyValues(row))
          if (valuePresent) then table.observe(slot, valueValues(row))
      row += 1
    }

  private final class GroupTable(initialCapacity: Int, aggs: Int32Aggs):
    private var capacity: Int = initialCapacity
    private var mask: Int = initialCapacity - 1
    private var occupied: Int = 0
    private var limit: Int = initialCapacity >>> 1
    private var missing: Int = 0
    private var keys: Array[Int] = new Array[Int](initialCapacity)
    private var state: Array[Byte] = new Array[Byte](initialCapacity)
    private var sums: Array[Long] =
      if (aggs.sum) new Array[Long](initialCapacity) else Array.emptyLongArray
    private var counts: Array[Long] =
      if (aggs.count) new Array[Long](initialCapacity) else Array.emptyLongArray
    private var mins: Array[Int] =
      if (aggs.min) new Array[Int](initialCapacity) else Array.emptyIntArray
    private var maxs: Array[Int] =
      if (aggs.max) new Array[Int](initialCapacity) else Array.emptyIntArray
    private var seen: Array[Byte] = new Array[Byte](initialCapacity)
    private var slots: Array[Int] = new Array[Int](16)
    private val hasSum: Boolean = aggs.sum
    private val hasCount: Boolean = aggs.count
    private val hasMin: Boolean = aggs.min
    private val hasMax: Boolean = aggs.max

    def addSum(key: Int, value: Long): Unit =
      accumulate(ensure(key), value)

    def accumulate(slot: Int, value: Long): Unit =
      sums(slot) += value
      notice(slot)

    def ensure(key: Int): Int =
      if (occupied >= limit) then grow()
      val slot = locate(key)
      if (state(slot) == 0) then
        state(slot) = 1
        keys(slot) = key
        missing += 1
        remember(slot)
      slot

    def observe(slot: Int, value: Int): Unit =
      if (seen(slot) == 0) then
        if (hasSum) then sums(slot) = value.toLong
        if (hasCount) then counts(slot) = 1L
        if (hasMin) then mins(slot) = value
        if (hasMax) then maxs(slot) = value
        notice(slot)
      else
        if (hasSum) then sums(slot) += value.toLong
        if (hasCount) then counts(slot) += 1L
        if (hasMin && value < mins(slot)) then mins(slot) = value
        if (hasMax && value > maxs(slot)) then maxs(slot) = value

    def compact(): ColumnBatch =
      val rows = occupied
      val outKeys = new Array[Int](rows)
      val outSums = if (hasSum) new Array[Long](rows) else Array.emptyLongArray
      val outCounts = if (hasCount) new Array[Long](rows) else Array.emptyLongArray
      val outMins = if (hasMin) new Array[Int](rows) else Array.emptyIntArray
      val outMaxs = if (hasMax) new Array[Int](rows) else Array.emptyIntArray
      val sharesValidity = hasSum || hasMin || hasMax
      val sparse = sharesValidity && missing != 0
      val words = if (sparse) Validity.allocate(rows) else Array.emptyLongArray
      var cursor = 0
      while (cursor < rows) {
        val slot = slots(cursor)
        outKeys(cursor) = keys(slot)
        if (seen(slot) != 0) then
          if (hasSum) then outSums(cursor) = sums(slot)
          if (hasCount) then outCounts(cursor) = counts(slot)
          if (hasMin) then outMins(cursor) = mins(slot)
          if (hasMax) then outMaxs(cursor) = maxs(slot)
          if (sparse) then Validity.setBit(words, cursor)
        cursor += 1
      }
      val keyColumn = Int32Column.of(outKeys, Validity.allValid(rows), rows)
      val valueValidity =
        if (sparse) Validity.fromWords(words, rows) else Validity.allValid(rows)
      // An unseen count stays 0 because `outCounts` is zero-filled.
      // Sum, min, and max share `valueValidity`. A published bitmap is not mutated.
      val measures = Vector(
        Option.when(hasSum) {
          LogicalType.Int64 -> (Int64Column.of(outSums, valueValidity, rows): Column)
        },
        Option.when(hasCount) {
          LogicalType.Int64 -> (Int64Column.of(outCounts, Validity.allValid(rows), rows): Column)
        },
        Option.when(hasMin) {
          LogicalType.Int32 -> (Int32Column.of(outMins, valueValidity, rows): Column)
        },
        Option.when(hasMax) {
          LogicalType.Int32 -> (Int32Column.of(outMaxs, valueValidity, rows): Column)
        },
      ).flatten
      new ColumnBatch(
        LogicalType.Int32 +: measures.map(_._1),
        keyColumn +: measures.map(_._2),
        rows,
      )

    private def notice(slot: Int): Unit =
      if (seen(slot) == 0) then
        seen(slot) = 1
        missing -= 1

    private def grow(): Unit =
      if (capacity >= ColumnHash.MaxCapacity) then
        throw new IllegalStateException(
          s"hash aggregate table is already at capacity $capacity",
        )
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
      sums = if (hasSum) new Array[Long](capacity) else Array.emptyLongArray
      counts = if (hasCount) new Array[Long](capacity) else Array.emptyLongArray
      mins = if (hasMin) new Array[Int](capacity) else Array.emptyIntArray
      maxs = if (hasMax) new Array[Int](capacity) else Array.emptyIntArray
      seen = new Array[Byte](capacity)
      occupied = 0
      var slot = 0
      while (slot < oldCapacity) {
        if (oldState(slot) != 0) then
          val placed = locate(oldKeys(slot))
          state(placed) = 1
          keys(placed) = oldKeys(slot)
          if (hasSum) then sums(placed) = oldSums(slot)
          if (hasCount) then counts(placed) = oldCounts(slot)
          if (hasMin) then mins(placed) = oldMins(slot)
          if (hasMax) then maxs(placed) = oldMaxs(slot)
          seen(placed) = oldSeen(slot)
          slots(occupied) = placed
          occupied += 1
        slot += 1
      }

    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot

    private def remember(slot: Int): Unit =
      if (occupied == slots.length) then
        val grown = slots.length << 1
        if (grown <= 0) then
          throw new IllegalStateException(s"group list is already at ${slots.length}")
        slots = Arrays.copyOf(slots, grown)
      slots(occupied) = slot
      occupied += 1

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

  private def emptySum: ColumnBatch =
    empty(Vector(LogicalType.Int32, LogicalType.Int64))

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
    val measures = Vector(
      Option.when(aggs.sum)(LogicalType.Int64),
      Option.when(aggs.count)(LogicalType.Int64),
      Option.when(aggs.min)(LogicalType.Int32),
      Option.when(aggs.max)(LogicalType.Int32),
    ).flatten
    LogicalType.Int32 +: measures
