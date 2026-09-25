package com.ewoodbury.sparklet.columnar

/**
 * Presence bits for a column. Bit 0 of word 0 is row 0. A set bit means the value is present; a
 * clear bit means null. The payload array is not consulted for a null row.
 *
 * `capacity` is the number of rows the bitmap can describe. A column may use a shorter live
 * `length`.
 */
final class Validity private (val words: Array[Long], val capacity: Int):

  def isValid(row: Int): Boolean =
    require(row >= 0 && row < capacity, s"row $row outside validity capacity $capacity")
    val word = words(row >>> 6)
    ((word >>> (row & 63)) & 1L) != 0L

object Validity:

  def allValid(capacity: Int): Validity =
    require(capacity >= 0, s"capacity must be >= 0, got $capacity")
    val words = new Array[Long](wordCount(capacity))
    java.util.Arrays.fill(words, -1L)
    val tail = capacity & 63
    if (tail != 0) then words(words.length - 1) = (1L << tail) - 1L
    new Validity(words, capacity)

  /** `present(row) == true` means row `row` is not null. */
  def pack(capacity: Int, present: Int => Boolean): Validity =
    require(capacity >= 0, s"capacity must be >= 0, got $capacity")
    val words = new Array[Long](wordCount(capacity))
    (0 until capacity).foreach { row =>
      if (present(row)) then
        val wordIndex = row >>> 6
        words(wordIndex) = words(wordIndex) | (1L << (row & 63))
    }
    new Validity(words, capacity)

  private def wordCount(capacity: Int): Int =
    (capacity + 63) >>> 6
