package com.ewoodbury.sparklet.columnar

/**
 * Filter and project over columns.
 *
 * Filter builds a selection of rows whose predicate column passes the predicate. For ints, longs,
 * floats, and bools, nulls are dropped and the predicate is not called. A string filter calls the
 * predicate on every row, including an empty cell, and an empty cell may survive. Survivors stay
 * in input order. Every column is compacted. The output capacity is the input live length, and the
 * output length is the survivor count. A string filter keeps the input dictionary.
 *
 * Project maps one column and shares the others. For the primitive columns, a null stays null, the
 * new slot stores `0`, and the function is not called. The mapped column reuses the input validity
 * bitmap, so null positions do not change. A string project calls the function on every row,
 * stores an empty result as an empty cell, and builds a new dictionary. Callers do not mutate a
 * published bitmap.
 *
 * Primitive loops keep the row index in a `var` so the bodies stay allocation-free. Each primitive
 * has its own loop so the copy stays monomorphic.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object ColumnKernel:

  trait Int32Predicate:
    def apply(value: Int): Boolean

  trait Int64Predicate:
    def apply(value: Long): Boolean

  trait Float64Predicate:
    def apply(value: Double): Boolean

  trait BoolPredicate:
    def apply(value: Boolean): Boolean

  trait Int32Map:
    def apply(value: Int): Int

  trait Int64Map:
    def apply(value: Long): Long

  trait Float64Map:
    def apply(value: Double): Double

  trait BoolMap:
    def apply(value: Boolean): Boolean

  trait Utf8Predicate:
    def apply(value: String): Boolean

  trait Utf8Map:
    def apply(value: String): String

  def filterInt32(batch: ColumnBatch, ordinal: Int, predicate: Int32Predicate): ColumnBatch =
    val column = asInt32(columnAt(batch, ordinal), ordinal)
    if (batch.columns.length == 1) then
      val scanned = scanInt32(column, predicate, record = false, selection = noSelection)
      publish(batch, scanned.column, scanned.count)
    else
      val selection = new Array[Int](batch.length)
      val scanned = scanInt32(column, predicate, record = true, selection = selection)
      assemble(batch, ordinal, scanned.column, selection, scanned.count)

  def filterInt64(batch: ColumnBatch, ordinal: Int, predicate: Int64Predicate): ColumnBatch =
    val column = asInt64(columnAt(batch, ordinal), ordinal)
    if (batch.columns.length == 1) then
      val scanned = scanInt64(column, predicate, record = false, selection = noSelection)
      publish(batch, scanned.column, scanned.count)
    else
      val selection = new Array[Int](batch.length)
      val scanned = scanInt64(column, predicate, record = true, selection = selection)
      assemble(batch, ordinal, scanned.column, selection, scanned.count)

  def filterFloat64(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: Float64Predicate,
  ): ColumnBatch =
    val column = asFloat64(columnAt(batch, ordinal), ordinal)
    if (batch.columns.length == 1) then
      val scanned = scanFloat64(column, predicate, record = false, selection = noSelection)
      publish(batch, scanned.column, scanned.count)
    else
      val selection = new Array[Int](batch.length)
      val scanned = scanFloat64(column, predicate, record = true, selection = selection)
      assemble(batch, ordinal, scanned.column, selection, scanned.count)

  def filterUtf8(batch: ColumnBatch, ordinal: Int, predicate: Utf8Predicate): ColumnBatch =
    val column = asUtf8(columnAt(batch, ordinal), ordinal)
    if (batch.columns.length == 1) then
      val scanned = scanUtf8(column, predicate, record = false, selection = noSelection)
      publish(batch, scanned.column, scanned.count)
    else
      val selection = new Array[Int](batch.length)
      val scanned = scanUtf8(column, predicate, record = true, selection = selection)
      assemble(batch, ordinal, scanned.column, selection, scanned.count)

  def filterBool(batch: ColumnBatch, ordinal: Int, predicate: BoolPredicate): ColumnBatch =
    val column = asBool(columnAt(batch, ordinal), ordinal)
    if (batch.columns.length == 1) then
      val scanned = scanBool(column, predicate, record = false, selection = noSelection)
      publish(batch, scanned.column, scanned.count)
    else
      val selection = new Array[Int](batch.length)
      val scanned = scanBool(column, predicate, record = true, selection = selection)
      assemble(batch, ordinal, scanned.column, selection, scanned.count)

  def projectInt32(batch: ColumnBatch, ordinal: Int, mapper: Int32Map): ColumnBatch =
    replace(batch, ordinal, mapInt32(asInt32(columnAt(batch, ordinal), ordinal), mapper))

  def projectInt64(batch: ColumnBatch, ordinal: Int, mapper: Int64Map): ColumnBatch =
    replace(batch, ordinal, mapInt64(asInt64(columnAt(batch, ordinal), ordinal), mapper))

  def projectFloat64(batch: ColumnBatch, ordinal: Int, mapper: Float64Map): ColumnBatch =
    replace(batch, ordinal, mapFloat64(asFloat64(columnAt(batch, ordinal), ordinal), mapper))

  def projectBool(batch: ColumnBatch, ordinal: Int, mapper: BoolMap): ColumnBatch =
    replace(batch, ordinal, mapBool(asBool(columnAt(batch, ordinal), ordinal), mapper))

  def projectUtf8(batch: ColumnBatch, ordinal: Int, mapper: Utf8Map): ColumnBatch =
    replace(batch, ordinal, mapUtf8(asUtf8(columnAt(batch, ordinal), ordinal), mapper))

  private val noSelection: Array[Int] = Array.emptyIntArray

  private final case class Scanned(column: Column, count: Int)

  private inline def scanInt32(
      column: Int32Column,
      predicate: Int32Predicate,
      inline record: Boolean,
      selection: Array[Int],
  ): Scanned =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Int](live)
    var written = 0
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        val value = in(row)
        if (predicate(value)) then
          inline if (record) then selection(written) = row
          out(written) = value
          written += 1
      row += 1
    }
    val built = Int32Column.of(out, Validity.prefixValid(live, written), written)
    Scanned(built, written)

  private inline def scanInt64(
      column: Int64Column,
      predicate: Int64Predicate,
      inline record: Boolean,
      selection: Array[Int],
  ): Scanned =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Long](live)
    var written = 0
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        val value = in(row)
        if (predicate(value)) then
          inline if (record) then selection(written) = row
          out(written) = value
          written += 1
      row += 1
    }
    val built = Int64Column.of(out, Validity.prefixValid(live, written), written)
    Scanned(built, written)

  private inline def scanFloat64(
      column: Float64Column,
      predicate: Float64Predicate,
      inline record: Boolean,
      selection: Array[Int],
  ): Scanned =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Double](live)
    var written = 0
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        val value = in(row)
        if (predicate(value)) then
          inline if (record) then selection(written) = row
          out(written) = value
          written += 1
      row += 1
    }
    val built = Float64Column.of(out, Validity.prefixValid(live, written), written)
    Scanned(built, written)

  private inline def scanBool(
      column: BoolColumn,
      predicate: BoolPredicate,
      inline record: Boolean,
      selection: Array[Int],
  ): Scanned =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Byte](live)
    var written = 0
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        val value = in(row) != 0
        if (predicate(value)) then
          inline if (record) then selection(written) = row
          out(written) = if (value) 1.toByte else 0.toByte
          written += 1
      row += 1
    }
    val built = BoolColumn.of(out, Validity.prefixValid(live, written), written)
    Scanned(built, written)

  private inline def scanUtf8(
      column: DictUtf8Column,
      predicate: Utf8Predicate,
      inline record: Boolean,
      selection: Array[Int],
  ): Scanned =
    val live = column.length
    val in = column.codes
    val words = column.validity.words
    val out = new Array[Int](live)
    val dstWords = Validity.allocate(live)
    var written = 0
    var row = 0
    while (row < live) {
      if (predicate(utf8String(column, row))) then
        inline if (record) then selection(written) = row
        if (Validity.isSet(words, row)) then
          out(written) = in(row)
          Validity.setBit(dstWords, written)
        written += 1
      row += 1
    }
    val built = DictUtf8Column.of(
      column.dictionary,
      out,
      Validity.fromWords(dstWords, live),
      written,
    )
    Scanned(built, written)

  private def mapInt32(column: Int32Column, mapper: Int32Map): Int32Column =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Int](in.length)
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then out(row) = mapper(in(row))
      row += 1
    }
    Int32Column.of(out, column.validity, live)

  private def mapInt64(column: Int64Column, mapper: Int64Map): Int64Column =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Long](in.length)
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then out(row) = mapper(in(row))
      row += 1
    }
    Int64Column.of(out, column.validity, live)

  private def mapFloat64(column: Float64Column, mapper: Float64Map): Float64Column =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Double](in.length)
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then out(row) = mapper(in(row))
      row += 1
    }
    Float64Column.of(out, column.validity, live)

  private def mapBool(column: BoolColumn, mapper: BoolMap): BoolColumn =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Byte](in.length)
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        out(row) = if (mapper(in(row) != 0)) 1.toByte else 0.toByte
      row += 1
    }
    BoolColumn.of(out, column.validity, live)

  private def mapUtf8(column: DictUtf8Column, mapper: Utf8Map): DictUtf8Column =
    val live = column.length
    val capacity = column.codes.length
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    val words = DictUtf8Column.Words.empty
    var row = 0
    while (row < live) {
      val text = mapper(utf8String(column, row))
      if (!BatchCodec.isAbsent(text)) then
        out(row) = words.intern(text)
        Validity.setBit(dstWords, row)
      row += 1
    }
    DictUtf8Column.of(words.toArray, out, Validity.fromWords(dstWords, capacity), live)

  /** An empty cell does not index the dictionary. */
  private def utf8String(column: DictUtf8Column, row: Int): String =
    if (Validity.isSet(column.validity.words, row)) then column.dictionary(column.codes(row))
    else BatchCodec.stringElement(None)

  private def assemble(
      batch: ColumnBatch,
      ordinal: Int,
      predicateColumn: Column,
      selection: Array[Int],
      count: Int,
  ): ColumnBatch =
    require(selection.length >= count, s"selection ${selection.length} shorter than $count rows")
    val capacity = batch.length
    val columns = Vector.tabulate(batch.columns.length) { index =>
      if (index == ordinal) then predicateColumn
      else compact(columnAt(batch, index), selection, count, capacity)
    }
    new ColumnBatch(batch.schema, columns, count)

  private def compact(
      column: Column,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): Column =
    column match
      case int32: Int32Column => compactInt32(int32, selection, count, capacity)
      case int64: Int64Column => compactInt64(int64, selection, count, capacity)
      case float64: Float64Column => compactFloat64(float64, selection, count, capacity)
      case bool: BoolColumn => compactBool(bool, selection, count, capacity)
      case utf8: DictUtf8Column => compactUtf8(utf8, selection, count, capacity)

  private def compactInt32(
      column: Int32Column,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): Int32Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    var index = 0
    while (index < count) {
      val row = selection(index)
      if (Validity.isSet(srcWords, row)) then
        out(index) = in(row)
        Validity.setBit(dstWords, index)
      index += 1
    }
    Int32Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def compactInt64(
      column: Int64Column,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): Int64Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Long](capacity)
    val dstWords = Validity.allocate(capacity)
    var index = 0
    while (index < count) {
      val row = selection(index)
      if (Validity.isSet(srcWords, row)) then
        out(index) = in(row)
        Validity.setBit(dstWords, index)
      index += 1
    }
    Int64Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def compactFloat64(
      column: Float64Column,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): Float64Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Double](capacity)
    val dstWords = Validity.allocate(capacity)
    var index = 0
    while (index < count) {
      val row = selection(index)
      if (Validity.isSet(srcWords, row)) then
        out(index) = in(row)
        Validity.setBit(dstWords, index)
      index += 1
    }
    Float64Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def compactBool(
      column: BoolColumn,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): BoolColumn =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Byte](capacity)
    val dstWords = Validity.allocate(capacity)
    var index = 0
    while (index < count) {
      val row = selection(index)
      if (Validity.isSet(srcWords, row)) then
        out(index) = if (in(row) != 0) 1.toByte else 0.toByte
        Validity.setBit(dstWords, index)
      index += 1
    }
    BoolColumn.of(out, Validity.fromWords(dstWords, capacity), count)

  private def compactUtf8(
      column: DictUtf8Column,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): DictUtf8Column =
    val in = column.codes
    val srcWords = column.validity.words
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    var index = 0
    while (index < count) {
      val row = selection(index)
      if (Validity.isSet(srcWords, row)) then
        out(index) = in(row)
        Validity.setBit(dstWords, index)
      index += 1
    }
    DictUtf8Column.of(column.dictionary, out, Validity.fromWords(dstWords, capacity), count)

  private def publish(batch: ColumnBatch, column: Column, count: Int): ColumnBatch =
    new ColumnBatch(batch.schema, Vector(column), count)

  private def replace(batch: ColumnBatch, ordinal: Int, column: Column): ColumnBatch =
    val columns = Vector.tabulate(batch.columns.length) { index =>
      if (index == ordinal) then column else columnAt(batch, index)
    }
    new ColumnBatch(batch.schema, columns, batch.length)

  private def columnAt(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse {
      throw new IllegalArgumentException(
        s"ordinal $ordinal outside width ${batch.columns.length}",
      )
    }

  private def asInt32(column: Column, ordinal: Int): Int32Column =
    column match
      case int32: Int32Column => int32
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Int32",
        )

  private def asInt64(column: Column, ordinal: Int): Int64Column =
    column match
      case int64: Int64Column => int64
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Int64",
        )

  private def asFloat64(column: Column, ordinal: Int): Float64Column =
    column match
      case float64: Float64Column => float64
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Float64",
        )

  private def asBool(column: Column, ordinal: Int): BoolColumn =
    column match
      case bool: BoolColumn => bool
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Bool",
        )

  private def asUtf8(column: Column, ordinal: Int): DictUtf8Column =
    column match
      case utf8: DictUtf8Column => utf8
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Utf8Dict",
        )
