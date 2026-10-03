package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Inner hash join of two batches on an `Int32` key. Null keys are dropped on both sides. A null
 * value is preserved on a matched row and stored as `0`.
 *
 * The build side is one open-addressed table of chains, so one build key can match many rows.
 * Output is `(key, build value, probe value)` in probe order, and within a probe row the build
 * chain is newest-first. That order is part of the contract.
 *
 * Each side names its key and value ordinals. The join is single-threaded. The probe writes
 * matches directly, with the row index in a `var`, so the hot loop stays allocation-free like
 * `ColumnKernel`. Output columns keep the grown capacity and `length` is the match count, so the
 * result is not copied down to size.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashJoin:

  /** One side of the join. `key` and `value` are column ordinals in `batch`. */
  final case class JoinSide(batch: ColumnBatch, key: Int, value: Int)

  def innerInt32(build: JoinSide, probe: JoinSide): ColumnBatch =
    val buildKeys = ColumnHash.int32At(build.batch, build.key)
    val buildValues = ColumnHash.int32At(build.batch, build.value)
    val probeKeys = ColumnHash.int32At(probe.batch, probe.key)
    val probeValues = ColumnHash.int32At(probe.batch, probe.value)
    require(buildKeys.length == buildValues.length, "build key and value columns differ in length")
    require(probeKeys.length == probeValues.length, "probe key and value columns differ in length")
    if (buildKeys.length == 0 || probeKeys.length == 0) then emptyJoin
    else indexed(buildKeys, buildValues).write(probeKeys, probeValues)

  private def indexed(buildKeys: Int32Column, buildValues: Int32Column): BuildTable =
    val table = new BuildTable(buildKeys, buildValues)
    table.insert()
    table

  private final class BuildTable(buildKeys: Int32Column, buildValues: Int32Column):
    private val rows: Int = buildKeys.length
    private val slotCapacity: Int = ColumnHash.tableSize(rows)
    private val mask: Int = slotCapacity - 1
    private val slotKey: Array[Int] = new Array[Int](slotCapacity)
    private val head: Array[Int] = new Array[Int](slotCapacity)
    private val next: Array[Int] = new Array[Int](rows)
    private val buildKeyValues: Array[Int] = buildKeys.values
    private val keyWords: Array[Long] = buildKeys.validity.words

    Arrays.fill(head, -1)
    Arrays.fill(next, -1)

    def insert(): Unit =
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          val key = buildKeyValues(row)
          var slot = ColumnHash.mix(key) & mask
          var placed = false
          while (!placed) {
            if (head(slot) == -1) then
              head(slot) = row
              slotKey(slot) = key
              placed = true
            else if (slotKey(slot) == key) then
              next(row) = head(slot)
              head(slot) = row
              placed = true
            else slot = (slot + 1) & mask
          }
        row += 1
      }

    def write(probeKeys: Int32Column, probeValues: Int32Column): ColumnBatch =
      var outCapacity = math.max(16, probeKeys.length)
      var filled = 0
      var outKeys = new Array[Int](outCapacity)
      var outBuild = new Array[Int](outCapacity)
      var outProbe = new Array[Int](outCapacity)
      var buildWords = Validity.allocate(outCapacity)
      var probeWords = Validity.allocate(outCapacity)
      val buildValueValues = buildValues.values
      val buildValueWords = buildValues.validity.words
      val probeValueValues = probeValues.values
      val probeValueWords = probeValues.validity.words
      val probeKeyValues = probeKeys.values
      val probeKeyWords = probeKeys.validity.words
      val probeRows = probeKeys.length
      var row = 0
      while (row < probeRows) {
        if (Validity.isSet(probeKeyWords, row)) then
          val key = probeKeyValues(row)
          var slot = ColumnHash.mix(key) & mask
          var resolved = false
          while (!resolved) {
            if (head(slot) == -1) then resolved = true
            else if (slotKey(slot) == key) then
              var buildRow = head(slot)
              while (buildRow != -1) {
                if (filled == outCapacity) then
                  outCapacity = doubled(outCapacity)
                  outKeys = Arrays.copyOf(outKeys, outCapacity)
                  outBuild = Arrays.copyOf(outBuild, outCapacity)
                  outProbe = Arrays.copyOf(outProbe, outCapacity)
                  buildWords = growWords(buildWords, outCapacity)
                  probeWords = growWords(probeWords, outCapacity)
                outKeys(filled) = buildKeyValues(buildRow)
                if (Validity.isSet(buildValueWords, buildRow)) then
                  outBuild(filled) = buildValueValues(buildRow)
                  Validity.setBit(buildWords, filled)
                if (Validity.isSet(probeValueWords, row)) then
                  outProbe(filled) = probeValueValues(row)
                  Validity.setBit(probeWords, filled)
                filled += 1
                buildRow = next(buildRow)
              }
              resolved = true
            else slot = (slot + 1) & mask
          }
        row += 1
      }
      if (filled == 0) then emptyJoin
      else publish(outKeys, outBuild, outProbe, buildWords, probeWords, outCapacity, filled)

  private def publish(
      keys: Array[Int],
      buildValues: Array[Int],
      probeValues: Array[Int],
      buildWords: Array[Long],
      probeWords: Array[Long],
      capacity: Int,
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.prefixValid(capacity, rows), rows),
        Int32Column.of(buildValues, Validity.fromWords(buildWords, capacity), rows),
        Int32Column.of(probeValues, Validity.fromWords(probeWords, capacity), rows),
      ),
      rows,
    )

  private def doubled(capacity: Int): Int =
    if (capacity >= ColumnHash.MaxCapacity) then
      throw new IllegalStateException(s"join output reached $capacity rows")
    capacity << 1

  private def growWords(words: Array[Long], rowCapacity: Int): Array[Long] =
    val needed = (rowCapacity + 63) >>> 6
    if (words.length == needed) words else Arrays.copyOf(words, needed)

  private def emptyJoin: ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
      ),
      0,
    )
