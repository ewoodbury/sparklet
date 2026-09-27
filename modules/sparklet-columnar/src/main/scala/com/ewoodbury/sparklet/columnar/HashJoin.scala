package com.ewoodbury.sparklet.columnar

import java.util.Arrays

/**
 * Inner hash join of two batches on an `Int32` key. Null keys are dropped on both sides. A null
 * value is preserved on a matched row and stored as `0`.
 *
 * The build side is one open-addressed table of chains, so one build key can match many rows.
 * Output is `(key, build value, probe value)` in probe order, and within a probe row the build
 * chain is newest-first. The join is single-threaded. Loops keep the row index in a `var`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashJoin:

  def innerInt32(
      build: ColumnBatch,
      buildKey: Int,
      buildValue: Int,
      probe: ColumnBatch,
      probeKey: Int,
      probeValue: Int,
  ): ColumnBatch =
    val buildKeys = int32At(build, buildKey)
    val buildValues = int32At(build, buildValue)
    val probeKeys = int32At(probe, probeKey)
    val probeValues = int32At(probe, probeValue)
    require(buildKeys.length == buildValues.length, "build key and value columns differ in length")
    require(probeKeys.length == probeValues.length, "probe key and value columns differ in length")
    if (buildKeys.length == 0 || probeKeys.length == 0) then emptyJoin
    else BuildTable(buildKeys, buildValues).write(probeKeys, probeValues)

  private final class BuildTable(buildKeys: Int32Column, buildValues: Int32Column):
    private val rows: Int = buildKeys.length
    private val capacity: Int = tableSize(rows)
    private val mask: Int = capacity - 1
    private val slotKey: Array[Int] = new Array[Int](capacity)
    private val head: Array[Int] = new Array[Int](capacity)
    private val next: Array[Int] = new Array[Int](rows)
    private val keyValues: Array[Int] = buildKeys.values
    private val keyWords: Array[Long] = buildKeys.validity.words

    Arrays.fill(head, -1)
    Arrays.fill(next, -1)
    insert()

    def write(probeKeys: Int32Column, probeValues: Int32Column): ColumnBatch =
      var capacity = math.max(16, probeKeys.length)
      var filled = 0
      var outKeys = new Array[Int](capacity)
      var outBuild = new Array[Int](capacity)
      var outProbe = new Array[Int](capacity)
      var buildPresent = new Array[Byte](capacity)
      var probePresent = new Array[Byte](capacity)
      val buildValueValues = buildValues.values
      val buildValueWords = buildValues.validity.words
      val probeValueValues = probeValues.values
      val probeValueWords = probeValues.validity.words
      val _ = walk(probeKeys) { (buildRow, probeRow) =>
        if (filled == capacity) then
          capacity = capacity << 1
          outKeys = Arrays.copyOf(outKeys, capacity)
          outBuild = Arrays.copyOf(outBuild, capacity)
          outProbe = Arrays.copyOf(outProbe, capacity)
          buildPresent = Arrays.copyOf(buildPresent, capacity)
          probePresent = Arrays.copyOf(probePresent, capacity)
        outKeys(filled) = keyValues(buildRow)
        if (Validity.isSet(buildValueWords, buildRow)) then
          outBuild(filled) = buildValueValues(buildRow)
          buildPresent(filled) = 1
        if (Validity.isSet(probeValueWords, probeRow)) then
          outProbe(filled) = probeValueValues(probeRow)
          probePresent(filled) = 1
        filled += 1
      }
      publish(outKeys, outBuild, outProbe, buildPresent, probePresent, filled)

    private def insert(): Unit =
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          val key = keyValues(row)
          var slot = mix(key) & mask
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

    private def walk(probeKeys: Int32Column)(visit: (Int, Int) => Unit): Int =
      val probeValues = probeKeys.values
      val probeWords = probeKeys.validity.words
      val probeRows = probeKeys.length
      var matches = 0
      var row = 0
      while (row < probeRows) {
        if (Validity.isSet(probeWords, row)) then
          val key = probeValues(row)
          var slot = mix(key) & mask
          var resolved = false
          while (!resolved) {
            if (head(slot) == -1) then resolved = true
            else if (slotKey(slot) == key) then
              var buildRow = head(slot)
              while (buildRow != -1) {
                visit(buildRow, row)
                matches += 1
                buildRow = next(buildRow)
              }
              resolved = true
            else slot = (slot + 1) & mask
          }
        row += 1
      }
      matches

  private def publish(
      keys: Array[Int],
      buildValues: Array[Int],
      probeValues: Array[Int],
      buildPresent: Array[Byte],
      probePresent: Array[Byte],
      rows: Int,
  ): ColumnBatch =
    val keyOut = Arrays.copyOf(keys, rows)
    val buildOut = Arrays.copyOf(buildValues, rows)
    val probeOut = Arrays.copyOf(probeValues, rows)
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keyOut, Validity.allValid(rows), rows),
        Int32Column.of(buildOut, presence(buildPresent, rows), rows),
        Int32Column.of(probeOut, presence(probePresent, rows), rows),
      ),
      rows,
    )

  private def presence(flags: Array[Byte], rows: Int): Validity =
    val words = Validity.allocate(rows)
    var row = 0
    while (row < rows) {
      if (flags(row) != 0) then Validity.setBit(words, row)
      row += 1
    }
    Validity.fromWords(words, rows)

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

  private def tableSize(rows: Int): Int =
    val need = if (rows > (1 << 29)) 1 << 30 else math.max(16, rows * 2)
    var capacity = 16
    while (capacity < need && capacity < (1 << 30)) capacity = capacity << 1
    capacity

  private def mix(key: Int): Int =
    var mixed = key
    mixed = (mixed ^ (mixed >>> 16)) * 0x7feb352d
    mixed = (mixed ^ (mixed >>> 15)) * 0x846ca68b
    mixed ^ (mixed >>> 16)

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
