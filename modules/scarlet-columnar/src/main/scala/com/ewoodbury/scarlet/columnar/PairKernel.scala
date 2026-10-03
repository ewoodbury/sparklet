package com.ewoodbury.scarlet.columnar

/**
 * Filter a two-column `Int32` pair. A null key or a null value is dropped, and the predicate is
 * not called on it. Survivors stay in input order.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object PairKernel:

  trait IntPairPredicate:
    def apply(key: Int, value: Int): Boolean

  def filter(batch: ColumnBatch, predicate: IntPairPredicate): ColumnBatch =
    require(
      batch.columns.length == 2,
      s"pair filter expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val values = int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    val rows = keys.length
    val outKeys = new Array[Int](rows)
    val outValues = new Array[Int](rows)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    var written = 0
    var row = 0
    while (row < rows) {
      if (Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row)) then
        val key = keyValues(row)
        val value = valueValues(row)
        if (predicate(key, value)) then
          outKeys(written) = key
          outValues(written) = value
          written += 1
      row += 1
    }
    val present = Validity.prefixValid(rows, written)
    new ColumnBatch(
      batch.schema,
      Vector(
        Int32Column.of(outKeys, present, written),
        Int32Column.of(outValues, present, written),
      ),
      written,
    )

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
