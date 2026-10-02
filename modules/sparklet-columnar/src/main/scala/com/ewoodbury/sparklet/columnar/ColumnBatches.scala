package com.ewoodbury.sparklet.columnar

/**
 * Slice a batch into morsels and concatenate batches that share a schema.
 *
 * A slice shorter than or equal to `batchSize` is the input batch itself. Concatenation of one
 * batch, or of only empty batches, returns the head batch. Copied ranges keep values and validity.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object ColumnBatches:

  def slice(batch: ColumnBatch, batchSize: Int): Vector[ColumnBatch] =
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val rows = batch.length
    if (rows <= batchSize) then Vector(batch)
    else
      val pieces = (rows + batchSize - 1) / batchSize
      Vector.tabulate(pieces) { piece =>
        val start = piece * batchSize
        val length = math.min(batchSize, rows - start)
        copy(batch, start, length)
      }

  def concat(parts: Vector[ColumnBatch]): ColumnBatch =
    require(parts.nonEmpty, "concat needs at least one batch")
    val head = parts.headOption.getOrElse(
      throw new IllegalArgumentException("concat needs at least one batch"),
    )
    if (parts.length == 1 || parts.forall(_.length == 0)) then head
    else
      parts.foreach { part =>
        require(
          part.schema.iterator.sameElements(head.schema.iterator),
          "concat batches disagree on schema",
        )
      }
      val total = parts.iterator.map(_.length).sum
      val columns = head.columns.indices.map { ordinal =>
        append(parts.map(part => columnAt(part, ordinal)), total)
      }.toVector
      new ColumnBatch(head.schema, columns, total)

  private def copy(batch: ColumnBatch, start: Int, length: Int): ColumnBatch =
    val columns = batch.columns.map(column => copyColumn(column, start, length))
    new ColumnBatch(batch.schema, columns, length)

  private def copyColumn(column: Column, start: Int, length: Int): Column =
    column match
      case int32: Int32Column =>
        val values = new Array[Int](length)
        System.arraycopy(int32.values, start, values, 0, length)
        Int32Column.of(values, validityRange(int32.validity, start, length), length)
      case int64: Int64Column =>
        val values = new Array[Long](length)
        System.arraycopy(int64.values, start, values, 0, length)
        Int64Column.of(values, validityRange(int64.validity, start, length), length)
      case float64: Float64Column =>
        val values = new Array[Double](length)
        System.arraycopy(float64.values, start, values, 0, length)
        Float64Column.of(values, validityRange(float64.validity, start, length), length)
      case bool: BoolColumn =>
        val values = new Array[Byte](length)
        System.arraycopy(bool.values, start, values, 0, length)
        BoolColumn.of(values, validityRange(bool.validity, start, length), length)
      case utf8: DictUtf8Column =>
        val codes = new Array[Int](length)
        System.arraycopy(utf8.codes, start, codes, 0, length)
        DictUtf8Column.of(
          utf8.dictionary,
          codes,
          validityRange(utf8.validity, start, length),
          length,
        )

  private def columnAt(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse {
      throw new IllegalArgumentException(
        s"ordinal $ordinal outside width ${batch.columns.length}",
      )
    }

  private def append(columns: Vector[Column], total: Int): Column =
    columns.headOption match
      case Some(_: Int32Column) =>
        appendInt32(columns.collect { case column: Int32Column => column }, total)
      case Some(_: Int64Column) =>
        appendInt64(columns.collect { case column: Int64Column => column }, total)
      case Some(_: Float64Column) =>
        appendFloat64(columns.collect { case column: Float64Column => column }, total)
      case Some(_: BoolColumn) =>
        appendBool(columns.collect { case column: BoolColumn => column }, total)
      case Some(_: DictUtf8Column) =>
        appendUtf8(columns.collect { case column: DictUtf8Column => column }, total)
      case None =>
        throw new IllegalArgumentException("concat needs a column")

  private def appendInt32(columns: Vector[Int32Column], total: Int): Int32Column =
    val values = new Array[Int](total)
    val words = Validity.allocate(total)
    val written = columns.foldLeft(0) { (offset, column) =>
      val length = column.length
      System.arraycopy(column.values, 0, values, offset, length)
      copyBits(column.validity, 0, words, offset, length)
      offset + length
    }
    require(written == total, s"copied $written rows into $total")
    Int32Column.of(values, Validity.fromWords(words, total), total)

  private def appendInt64(columns: Vector[Int64Column], total: Int): Int64Column =
    val values = new Array[Long](total)
    val words = Validity.allocate(total)
    val written = columns.foldLeft(0) { (offset, column) =>
      val length = column.length
      System.arraycopy(column.values, 0, values, offset, length)
      copyBits(column.validity, 0, words, offset, length)
      offset + length
    }
    require(written == total, s"copied $written rows into $total")
    Int64Column.of(values, Validity.fromWords(words, total), total)

  private def appendFloat64(columns: Vector[Float64Column], total: Int): Float64Column =
    val values = new Array[Double](total)
    val words = Validity.allocate(total)
    val written = columns.foldLeft(0) { (offset, column) =>
      val length = column.length
      System.arraycopy(column.values, 0, values, offset, length)
      copyBits(column.validity, 0, words, offset, length)
      offset + length
    }
    require(written == total, s"copied $written rows into $total")
    Float64Column.of(values, Validity.fromWords(words, total), total)

  private def appendBool(columns: Vector[BoolColumn], total: Int): BoolColumn =
    val values = new Array[Byte](total)
    val words = Validity.allocate(total)
    val written = columns.foldLeft(0) { (offset, column) =>
      val length = column.length
      System.arraycopy(column.values, 0, values, offset, length)
      copyBits(column.validity, 0, words, offset, length)
      offset + length
    }
    require(written == total, s"copied $written rows into $total")
    BoolColumn.of(values, Validity.fromWords(words, total), total)

  private def appendUtf8(columns: Vector[DictUtf8Column], total: Int): DictUtf8Column =
    val head = columns.headOption.getOrElse {
      throw new IllegalArgumentException("concat needs a column")
    }
    val codes = new Array[Int](total)
    val words = Validity.allocate(total)
    if (columns.forall(column => sameDictionary(column.dictionary, head.dictionary))) then
      val written = columns.foldLeft(0) { (offset, column) =>
        val length = column.length
        System.arraycopy(column.codes, 0, codes, offset, length)
        copyBits(column.validity, 0, words, offset, length)
        offset + length
      }
      require(written == total, s"copied $written rows into $total")
      DictUtf8Column.of(head.dictionary, codes, Validity.fromWords(words, total), total)
    else remapUtf8(columns, codes, words, total)

  /**
   * The same array, or the same strings in the same order. Reference equality is the slice,
   * filter, and exchange case.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def sameDictionary(left: Array[String], right: Array[String]): Boolean =
    (left eq right) || left.sameElements(right)

  /** Intern each used dictionary entry once, then translate rows through that code map. */
  private def remapUtf8(
      columns: Vector[DictUtf8Column],
      codes: Array[Int],
      dstWords: Array[Long],
      total: Int,
  ): DictUtf8Column =
    val pieces = columns.toArray
    val remaps = new Array[Array[Int]](pieces.length)
    val acc = DictUtf8Column.Words.empty
    var index = 0
    while (index < pieces.length) {
      val column = pieces(index)
      val dictionary = column.dictionary
      val used = usedCodes(column)
      val remap = new Array[Int](dictionary.length)
      var code = 0
      while (code < dictionary.length) {
        if (used(code)) then remap(code) = acc.intern(dictionary(code))
        code += 1
      }
      remaps(index) = remap
      index += 1
    }
    var offset = 0
    index = 0
    while (index < pieces.length) {
      val column = pieces(index)
      val remap = remaps(index)
      val src = column.codes
      val srcWords = column.validity.words
      val live = column.length
      var row = 0
      while (row < live) {
        if (Validity.isSet(srcWords, row)) then
          codes(offset + row) = remap(src(row))
          Validity.setBit(dstWords, offset + row)
        row += 1
      }
      offset += live
      index += 1
    }
    require(offset == total, s"copied $offset rows into $total")
    DictUtf8Column.of(acc.toArray, codes, Validity.fromWords(dstWords, total), total)

  private def usedCodes(column: DictUtf8Column): Array[Boolean] =
    val used = new Array[Boolean](column.dictionary.length)
    val src = column.codes
    val srcWords = column.validity.words
    val live = column.length
    var row = 0
    while (row < live) {
      if (Validity.isSet(srcWords, row)) then used(src(row)) = true
      row += 1
    }
    used

  private def validityRange(from: Validity, start: Int, length: Int): Validity =
    val words = Validity.allocate(length)
    copyBits(from, start, words, 0, length)
    Validity.fromWords(words, length)

  private def copyBits(
      from: Validity,
      fromStart: Int,
      into: Array[Long],
      intoStart: Int,
      length: Int,
  ): Unit =
    var src = fromStart
    var dst = intoStart
    var remaining = length
    val dstShift = dst & 63
    if (dstShift != 0 && remaining > 0) then
      val head = math.min(64 - dstShift, remaining)
      orBits(into, dst, readBits(from.words, src, head))
      src += head
      dst += head
      remaining -= head
    while (remaining >= 64) {
      orBits(into, dst, readBits(from.words, src, 64))
      src += 64
      dst += 64
      remaining -= 64
    }
    if (remaining > 0) then orBits(into, dst, readBits(from.words, src, remaining))

  /**
   * `width` is 1 to 64. A full word is the source word itself: `1L << 64` is masked to a 1-bit
   * shift, so it is not a 64-bit mask.
   */
  private def readBits(words: Array[Long], bit: Int, width: Int): Long =
    val wordIndex = bit >>> 6
    val shift = bit & 63
    if (shift == 0) then
      if (width == 64) then words(wordIndex)
      else words(wordIndex) & ((1L << width) - 1L)
    else
      val low = words(wordIndex) >>> shift
      val lowWidth = 64 - shift
      if (width <= lowWidth) then low & ((1L << width) - 1L)
      else
        val highWidth = width - lowWidth
        val high = words(wordIndex + 1) & ((1L << highWidth) - 1L)
        low | (high << lowWidth)

  /**
   * `chunk` holds `width` low bits, and `bit` starts far enough from the next word to fit them.
   */
  private def orBits(words: Array[Long], bit: Int, chunk: Long): Unit =
    val wordIndex = bit >>> 6
    val shift = bit & 63
    words(wordIndex) = words(wordIndex) | (chunk << shift)
