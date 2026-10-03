package com.ewoodbury.scarlet.columnar

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
 * has its own loop so the copy stays monomorphic. A pipeline loop expands one filter/project
 * pattern at its call site. Int and float nulls are dropped by a filter and skipped by a project,
 * and those functions are not called. A string pipeline calls its functions on empty cells. A
 * filter-only string pipeline keeps the input dictionary.
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

  def pipelineInt32FF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value => if (first(value) && second(value)) then accepted(value) else 0L,
    )

  def pipelineInt32FP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
  ): ColumnBatch =
    fuseInt32(batch, true, value => if (first(value)) then accepted(second(value)) else 0L)

  def pipelineInt32PF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then accepted(mapped) else 0L,
    )

  def pipelineInt32PP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
  ): ColumnBatch = fuseInt32(batch, false, value => accepted(second(first(value))))

  def pipelineInt32FFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value => if (first(value) && second(value) && third(value)) then accepted(value) else 0L,
    )

  def pipelineInt32FFP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value => if (first(value) && second(value)) then accepted(third(value)) else 0L,
    )

  def pipelineInt32FPF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def pipelineInt32FPP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
  ): ColumnBatch =
    fuseInt32(batch, true, value => if (first(value)) then accepted(third(second(value))) else 0L)

  def pipelineInt32PFF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped)) then accepted(mapped) else 0L,
    )

  def pipelineInt32PFP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then accepted(third(mapped)) else 0L,
    )

  def pipelineInt32PPF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped)) then accepted(mapped) else 0L,
    )

  def pipelineInt32PPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
  ): ColumnBatch = fuseInt32(batch, false, value => accepted(third(second(first(value)))))

  def pipelineInt32FFFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value) && second(value) && third(value) && fourth(value)) then accepted(value)
        else 0L,
    )

  def pipelineInt32FFFP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value) && second(value) && third(value)) then accepted(fourth(value)) else 0L,
    )

  def pipelineInt32FFPF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value) && second(value)) then
          val mapped = third(value)
          if (fourth(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def pipelineInt32FFPP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value => if (first(value) && second(value)) then accepted(fourth(third(value))) else 0L,
    )

  def pipelineInt32FPFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped) && fourth(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def pipelineInt32FPFP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped)) then accepted(fourth(mapped)) else 0L
        else 0L,
    )

  def pipelineInt32FPPF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = third(second(value))
          if (fourth(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def pipelineInt32FPPP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value => if (first(value)) then accepted(fourth(third(second(value)))) else 0L,
    )

  def pipelineInt32PFFF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped) && fourth(mapped)) then accepted(mapped) else 0L,
    )

  def pipelineInt32PFFP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped)) then accepted(fourth(mapped)) else 0L,
    )

  def pipelineInt32PFPF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then
          val next = third(mapped)
          if (fourth(next)) then accepted(next) else 0L
        else 0L,
    )

  def pipelineInt32PFPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then accepted(fourth(third(mapped))) else 0L,
    )

  def pipelineInt32PPFF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped) && fourth(mapped)) then accepted(mapped) else 0L,
    )

  def pipelineInt32PPFP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped)) then accepted(fourth(mapped)) else 0L,
    )

  def pipelineInt32PPPF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = third(second(first(value)))
        if (fourth(mapped)) then accepted(mapped) else 0L,
    )

  def pipelineInt32PPPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(batch, false, value => accepted(fourth(third(second(first(value))))))

  private inline def fuseInt32(
      batch: ColumnBatch,
      inline compact: Boolean,
      inline process: Int => Long,
  ): ColumnBatch =
    require(
      batch.columns.length == 1,
      s"int pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = asInt32(columnAt(batch, 0), 0)
    val live = column.length
    val in = column.values
    val words = column.validity.words
    // Compaction only needs live-row capacity; map-only pipelines preserve input capacity.
    val capacity = inline if (compact) then live else in.length
    val out = new Array[Int](capacity)
    var written = 0
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        val result = process(in(row))
        inline if (compact) then
          if ((result & 1L) != 0L) then
            out(written) = (result >> 1).toInt
            written += 1
        else out(row) = (result >> 1).toInt
      row += 1
    }
    val length = inline if (compact) then written else live
    val validity =
      inline if (compact) then Validity.prefixValid(capacity, written)
      else column.validity
    val result = Int32Column.of(out, validity, length)
    new ColumnBatch(batch.schema, Vector(result), length)

  // The low bit marks a surviving row; the remaining bits preserve the signed int payload.
  private inline def accepted(value: Int): Long = (value.toLong << 1) | 1L

  def pipelineFloat64FF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then if (second(value)) then acceptFloat(gate, value) else false
        else false,
    )

  def pipelineFloat64PF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        val mapped0 = first(value)
        if (second(mapped0)) then acceptFloat(gate, mapped0) else false,
    )

  def pipelineFloat64FP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) => if (first(value)) then acceptFloat(gate, second(value)) else false,
    )

  def pipelineFloat64PP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      false,
      (value, gate) =>
        val mapped0 = first(value)
        acceptFloat(gate, second(mapped0)),
    )

  def pipelineFloat64FFF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then
          if (second(value)) then if (third(value)) then acceptFloat(gate, value) else false
          else false
        else false,
    )

  def pipelineFloat64PFF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        val mapped0 = first(value)
        if (second(mapped0)) then if (third(mapped0)) then acceptFloat(gate, mapped0) else false
        else false,
    )

  def pipelineFloat64FPF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then
          val mapped1 = second(value)
          if (third(mapped1)) then acceptFloat(gate, mapped1) else false
        else false,
    )

  def pipelineFloat64PPF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        val mapped0 = first(value)
        val mapped1 = second(mapped0)
        if (third(mapped1)) then acceptFloat(gate, mapped1) else false,
    )

  def pipelineFloat64FFP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then if (second(value)) then acceptFloat(gate, third(value)) else false
        else false,
    )

  def pipelineFloat64PFP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        val mapped0 = first(value)
        if (second(mapped0)) then acceptFloat(gate, third(mapped0)) else false,
    )

  def pipelineFloat64FPP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then
          val mapped1 = second(value)
          acceptFloat(gate, third(mapped1))
        else false,
    )

  def pipelineFloat64PPP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      false,
      (value, gate) =>
        val mapped0 = first(value)
        val mapped1 = second(mapped0)
        acceptFloat(gate, third(mapped1)),
    )

  def pipelineFloat64FFFF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then
          if (second(value)) then
            if (third(value)) then if (fourth(value)) then acceptFloat(gate, value) else false
            else false
          else false
        else false,
    )

  def pipelineFloat64PFFF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        val mapped0 = first(value)
        if (second(mapped0)) then
          if (third(mapped0)) then if (fourth(mapped0)) then acceptFloat(gate, mapped0) else false
          else false
        else false,
    )

  def pipelineFloat64FPFF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then
          val mapped1 = second(value)
          if (third(mapped1)) then if (fourth(mapped1)) then acceptFloat(gate, mapped1) else false
          else false
        else false,
    )

  def pipelineFloat64PPFF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        val mapped0 = first(value)
        val mapped1 = second(mapped0)
        if (third(mapped1)) then if (fourth(mapped1)) then acceptFloat(gate, mapped1) else false
        else false,
    )

  def pipelineFloat64FFPF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then
          if (second(value)) then
            val mapped2 = third(value)
            if (fourth(mapped2)) then acceptFloat(gate, mapped2) else false
          else false
        else false,
    )

  def pipelineFloat64PFPF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        val mapped0 = first(value)
        if (second(mapped0)) then
          val mapped2 = third(mapped0)
          if (fourth(mapped2)) then acceptFloat(gate, mapped2) else false
        else false,
    )

  def pipelineFloat64FPPF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then
          val mapped1 = second(value)
          val mapped2 = third(mapped1)
          if (fourth(mapped2)) then acceptFloat(gate, mapped2) else false
        else false,
    )

  def pipelineFloat64PPPF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        val mapped0 = first(value)
        val mapped1 = second(mapped0)
        val mapped2 = third(mapped1)
        if (fourth(mapped2)) then acceptFloat(gate, mapped2) else false,
    )

  def pipelineFloat64FFFP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then
          if (second(value)) then
            if (third(value)) then acceptFloat(gate, fourth(value)) else false
          else false
        else false,
    )

  def pipelineFloat64PFFP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        val mapped0 = first(value)
        if (second(mapped0)) then
          if (third(mapped0)) then acceptFloat(gate, fourth(mapped0)) else false
        else false,
    )

  def pipelineFloat64FPFP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then
          val mapped1 = second(value)
          if (third(mapped1)) then acceptFloat(gate, fourth(mapped1)) else false
        else false,
    )

  def pipelineFloat64PPFP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        val mapped0 = first(value)
        val mapped1 = second(mapped0)
        if (third(mapped1)) then acceptFloat(gate, fourth(mapped1)) else false,
    )

  def pipelineFloat64FFPP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then
          if (second(value)) then
            val mapped2 = third(value)
            acceptFloat(gate, fourth(mapped2))
          else false
        else false,
    )

  def pipelineFloat64PFPP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        val mapped0 = first(value)
        if (second(mapped0)) then
          val mapped2 = third(mapped0)
          acceptFloat(gate, fourth(mapped2))
        else false,
    )

  def pipelineFloat64FPPP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      true,
      (value, gate) =>
        if (first(value)) then
          val mapped1 = second(value)
          val mapped2 = third(mapped1)
          acceptFloat(gate, fourth(mapped2))
        else false,
    )

  def pipelineFloat64PPPP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuseFloat64(
      batch,
      false,
      (value, gate) =>
        val mapped0 = first(value)
        val mapped1 = second(mapped0)
        val mapped2 = third(mapped1)
        acceptFloat(gate, fourth(mapped2)),
    )

  def pipelineUtf8FF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Filters(batch, text => first(text) && second(text))

  def pipelineUtf8PF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        val mapped0 = first(text)
        if (second(mapped0)) then acceptUtf8(gate, mapped0) else false,
    )

  def pipelineUtf8FP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) => if (first(text)) then acceptUtf8(gate, second(text)) else false,
    )

  def pipelineUtf8PP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      false,
      (text, gate) =>
        val mapped0 = first(text)
        acceptUtf8(gate, second(mapped0)),
    )

  def pipelineUtf8FFF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Filters(batch, text => first(text) && second(text) && third(text))

  def pipelineUtf8PFF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        val mapped0 = first(text)
        if (second(mapped0)) then if (third(mapped0)) then acceptUtf8(gate, mapped0) else false
        else false,
    )

  def pipelineUtf8FPF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        if (first(text)) then
          val mapped1 = second(text)
          if (third(mapped1)) then acceptUtf8(gate, mapped1) else false
        else false,
    )

  def pipelineUtf8PPF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        val mapped0 = first(text)
        val mapped1 = second(mapped0)
        if (third(mapped1)) then acceptUtf8(gate, mapped1) else false,
    )

  def pipelineUtf8FFP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        if (first(text)) then if (second(text)) then acceptUtf8(gate, third(text)) else false
        else false,
    )

  def pipelineUtf8PFP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        val mapped0 = first(text)
        if (second(mapped0)) then acceptUtf8(gate, third(mapped0)) else false,
    )

  def pipelineUtf8FPP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        if (first(text)) then
          val mapped1 = second(text)
          acceptUtf8(gate, third(mapped1))
        else false,
    )

  def pipelineUtf8PPP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      false,
      (text, gate) =>
        val mapped0 = first(text)
        val mapped1 = second(mapped0)
        acceptUtf8(gate, third(mapped1)),
    )

  def pipelineUtf8FFFF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Filters(batch, text => first(text) && second(text) && third(text) && fourth(text))

  def pipelineUtf8PFFF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        val mapped0 = first(text)
        if (second(mapped0)) then
          if (third(mapped0)) then if (fourth(mapped0)) then acceptUtf8(gate, mapped0) else false
          else false
        else false,
    )

  def pipelineUtf8FPFF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        if (first(text)) then
          val mapped1 = second(text)
          if (third(mapped1)) then if (fourth(mapped1)) then acceptUtf8(gate, mapped1) else false
          else false
        else false,
    )

  def pipelineUtf8PPFF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        val mapped0 = first(text)
        val mapped1 = second(mapped0)
        if (third(mapped1)) then if (fourth(mapped1)) then acceptUtf8(gate, mapped1) else false
        else false,
    )

  def pipelineUtf8FFPF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        if (first(text)) then
          if (second(text)) then
            val mapped2 = third(text)
            if (fourth(mapped2)) then acceptUtf8(gate, mapped2) else false
          else false
        else false,
    )

  def pipelineUtf8PFPF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        val mapped0 = first(text)
        if (second(mapped0)) then
          val mapped2 = third(mapped0)
          if (fourth(mapped2)) then acceptUtf8(gate, mapped2) else false
        else false,
    )

  def pipelineUtf8FPPF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        if (first(text)) then
          val mapped1 = second(text)
          val mapped2 = third(mapped1)
          if (fourth(mapped2)) then acceptUtf8(gate, mapped2) else false
        else false,
    )

  def pipelineUtf8PPPF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        val mapped0 = first(text)
        val mapped1 = second(mapped0)
        val mapped2 = third(mapped1)
        if (fourth(mapped2)) then acceptUtf8(gate, mapped2) else false,
    )

  def pipelineUtf8FFFP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        if (first(text)) then
          if (second(text)) then if (third(text)) then acceptUtf8(gate, fourth(text)) else false
          else false
        else false,
    )

  def pipelineUtf8PFFP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        val mapped0 = first(text)
        if (second(mapped0)) then
          if (third(mapped0)) then acceptUtf8(gate, fourth(mapped0)) else false
        else false,
    )

  def pipelineUtf8FPFP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        if (first(text)) then
          val mapped1 = second(text)
          if (third(mapped1)) then acceptUtf8(gate, fourth(mapped1)) else false
        else false,
    )

  def pipelineUtf8PPFP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        val mapped0 = first(text)
        val mapped1 = second(mapped0)
        if (third(mapped1)) then acceptUtf8(gate, fourth(mapped1)) else false,
    )

  def pipelineUtf8FFPP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        if (first(text)) then
          if (second(text)) then
            val mapped2 = third(text)
            acceptUtf8(gate, fourth(mapped2))
          else false
        else false,
    )

  def pipelineUtf8PFPP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        val mapped0 = first(text)
        if (second(mapped0)) then
          val mapped2 = third(mapped0)
          acceptUtf8(gate, fourth(mapped2))
        else false,
    )

  def pipelineUtf8FPPP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      true,
      (text, gate) =>
        if (first(text)) then
          val mapped1 = second(text)
          val mapped2 = third(mapped1)
          acceptUtf8(gate, fourth(mapped2))
        else false,
    )

  def pipelineUtf8PPPP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseUtf8Mapped(
      batch,
      false,
      (text, gate) =>
        val mapped0 = first(text)
        val mapped1 = second(mapped0)
        val mapped2 = third(mapped1)
        acceptUtf8(gate, fourth(mapped2)),
    )

  /**
   * One gate per batch, outside the row loop. An int packs the keep bit beside the payload. A
   * double and a string return that decision and store only the payload here.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class FloatGate:
    var value: Double = 0.0

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class Utf8Gate:
    var present: Boolean = false
    var text: String = ""

  /** Returns whether the row survives, so a pattern that forgets the decision does not compile. */
  private inline def acceptFloat(gate: FloatGate, value: Double): Boolean =
    gate.value = value
    true

  /** Returns whether the row survives, so a pattern that forgets the decision does not compile. */
  private inline def acceptUtf8(gate: Utf8Gate, value: String): Boolean =
    if (BatchCodec.isAbsent(value)) then gate.present = false
    else
      gate.present = true
      gate.text = value
    true

  private inline def fuseFloat64(
      batch: ColumnBatch,
      inline compact: Boolean,
      inline step: (Double, FloatGate) => Boolean,
  ): ColumnBatch =
    require(
      batch.columns.length == 1,
      s"float pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = asFloat64(columnAt(batch, 0), 0)
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val capacity = inline if (compact) then live else in.length
    val out = new Array[Double](capacity)
    val gate = new FloatGate
    var written = 0
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        val kept = step(in(row), gate)
        inline if (compact) then
          if (kept) then
            out(written) = gate.value
            written += 1
        else if (kept) then out(row) = gate.value
      row += 1
    }
    val length = inline if (compact) then written else live
    val validity =
      inline if (compact) then Validity.prefixValid(capacity, written) else column.validity
    val result = Float64Column.of(out, validity, length)
    new ColumnBatch(batch.schema, Vector(result), length)

  private inline def fuseUtf8Filters(
      batch: ColumnBatch,
      inline keep: String => Boolean,
  ): ColumnBatch =
    require(
      batch.columns.length == 1,
      s"utf8 pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = asUtf8(columnAt(batch, 0), 0)
    val live = column.length
    val codes = column.codes
    val words = column.validity.words
    val out = new Array[Int](live)
    val dstWords = Validity.allocate(live)
    var written = 0
    var row = 0
    while (row < live) {
      if (keep(utf8String(column, row))) then
        if (Validity.isSet(words, row)) then
          out(written) = codes(row)
          Validity.setBit(dstWords, written)
        written += 1
      row += 1
    }
    val result = DictUtf8Column.of(
      column.dictionary,
      out,
      Validity.fromWords(dstWords, live),
      written,
    )
    new ColumnBatch(batch.schema, Vector(result), written)

  /**
   * Projects build one dictionary of the strings this loop stores. A later filter therefore does
   * not retain a string it dropped, because the intermediate column never existed.
   */
  private inline def fuseUtf8Mapped(
      batch: ColumnBatch,
      inline compact: Boolean,
      inline step: (String, Utf8Gate) => Boolean,
  ): ColumnBatch =
    require(
      batch.columns.length == 1,
      s"utf8 pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = asUtf8(columnAt(batch, 0), 0)
    val live = column.length
    val capacity = inline if (compact) then live else column.codes.length
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    val words = DictUtf8Column.Words.empty
    val gate = new Utf8Gate
    var written = 0
    var row = 0
    while (row < live) {
      gate.present = false
      val kept = step(utf8String(column, row), gate)
      inline if (compact) then
        if (kept) then
          if (gate.present) then
            out(written) = words.intern(gate.text)
            Validity.setBit(dstWords, written)
          written += 1
      else if (kept && gate.present) then
        out(row) = words.intern(gate.text)
        Validity.setBit(dstWords, row)
      row += 1
    }
    val length = inline if (compact) then written else live
    val result =
      DictUtf8Column.of(words.toArray, out, Validity.fromWords(dstWords, capacity), length)
    new ColumnBatch(batch.schema, Vector(result), length)

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
