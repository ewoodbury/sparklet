package com.ewoodbury.scarlet.columnar

import java.lang.management.ManagementFactory

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Profiles one-batch hash aggregation. Not a test.
 *
 * The timed region includes the hash table allocation and the checksum of the groups. Inputs are
 * built beforehand. The DistCollection `reduceByKey` sample opts out of columnar execution.
 * `fused` applies `value > 0` and `value * 2` inside the aggregate scan. `staged`
 * does that with `ColumnKernel` and then sums.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.columnar.HashStudy"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashStudy:

  private val warmup = 3
  private val measure = 7

  private object Positive extends ColumnKernel.Int32Predicate:
    def apply(value: Int): Boolean = value > 0

  private object TimesTwo extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value * 2

  def main(args: Array[String]): Unit =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    threadBean.foreach(_.setThreadAllocatedMemoryEnabled(true))
    println("hash aggregate study warmup=3 measure=7")
    val rows = 1000000
    Array(16, 1024, 100000, 1000000).foreach { cardinality =>
      val columns = generate(rows, cardinality)
      val batch = batchOf(columns)
      val expected = expect(columns, filtered = false)
      emit(s"sum-card$cardinality", rows, expected) {
        val grouped = HashAggregate.sumInt32(batch, 0, 1)
        checksum(grouped)
      }
      val filteredExpected = expect(columns, filtered = true)
      emit(s"fused-card$cardinality", rows, filteredExpected) {
        val grouped = HashAggregate.sumInt32(batch, 0, 1, Positive, TimesTwo)
        checksum(grouped)
      }
      emit(s"staged-card$cardinality", rows, filteredExpected) {
        val filtered = ColumnKernel.filterInt32(batch, 1, Positive)
        val projected = ColumnKernel.projectInt32(filtered, 1, TimesTwo)
        checksum(HashAggregate.sumInt32(projected, 0, 1))
      }
    }
    val combined = generate(rows, 1024)
    val combinedBatch = batchOf(combined)
    val combinedExpected = expect(combined, filtered = false)
    emit("sum-count-min-max-card1024", rows, combinedExpected) {
      val grouped = HashAggregate.aggregateInt32(
        combinedBatch,
        0,
        1,
        HashAggregate.Int32Aggs(sum = true, count = true, min = true, max = true),
      )
      checksum(grouped)
    }
    rowEngine(100000, 1024)
    rowEngine(1000000, 1024)

  private def rowEngine(rows: Int, cardinality: Int): Unit =
    val columns = generate(rows, cardinality)
    val pairs = (0 until rows).map { row => (columns.keys(row), columns.values(row)) }.toVector
    val expected = expect(columns, filtered = false)
    val plan = DistCollection(pairs, 4).reduceByKey((left: Int, right: Int) => left + right)
    emit(s"reduceByKey-n$rows-card$cardinality", rows, expected) {
      plan.collect().foldLeft(0L) { case (sum, (_, value)) => sum + value.toLong }
    }

  private def emit(name: String, rows: Int, expected: Long)(body: => Long): Unit =
    System.err.println(s"run $name")
    (0 until warmup).foreach { _ =>
      val got = body
      require(got == expected, s"$name warmup checksum $got != $expected")
    }
    val samples = new Array[Long](measure)
    val before = allocatedBytes
    (0 until measure).foreach { sample =>
      val start = System.nanoTime()
      val got = body
      val elapsed = System.nanoTime() - start
      require(got == expected, s"$name checksum $got != $expected")
      samples(sample) = elapsed
    }
    val alloc = (allocatedBytes - before) / measure.toLong
    val p50 = percentile(samples)
    val rate = if (p50 <= 0L) 0L else (rows.toLong * 1000000000L) / p50
    println(s"$name\trows=$rows\tp50_ns=$p50\trows_per_s=$rate\talloc_bytes=$alloc\tchecksum=$expected")

  private def checksum(batch: ColumnBatch): Long =
    batch.columns.lift(1) match
      case Some(column: Int64Column) =>
        val values = column.values
        var index = 0
        var sum = 0L
        while (index < column.length) {
          if (column.isValid(index)) then sum += values(index)
          index += 1
        }
        sum
      case _ =>
        throw new IllegalStateException(s"expected a sum column, got ${batch.schema}")

  private final case class Columns(keys: Array[Int], values: Array[Int])

  private def generate(rows: Int, cardinality: Int): Columns =
    val keys = new Array[Int](rows)
    val values = new Array[Int](rows)
    val shift = rows / 2
    var index = 0
    while (index < rows) {
      keys(index) = index % cardinality
      values(index) = index - shift
      index += 1
    }
    Columns(keys, values)

  private def batchOf(columns: Columns): ColumnBatch =
    val rows = columns.keys.length
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(columns.keys, Validity.allValid(rows), rows),
        Int32Column.of(columns.values, Validity.allValid(rows), rows),
      ),
      rows,
    )

  private def expect(columns: Columns, filtered: Boolean): Long =
    var index = 0
    var sum = 0L
    while (index < columns.values.length) {
      val value = columns.values(index)
      if (!filtered || value > 0) then sum += (if (filtered) value.toLong * 2L else value.toLong)
      index += 1
    }
    sum

  private def percentile(samples: Array[Long]): Long =
    val sorted = samples.clone()
    java.util.Arrays.sort(sorted)
    sorted(sorted.length / 2)

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None

  private def allocatedBytes: Long =
    threadBean match
      case Some(bean) =>
        val ids = bean.getAllThreadIds
        var index = 0
        var total = 0L
        while (index < ids.length) {
          val bytes = bean.getThreadAllocatedBytes(ids(index))
          if (bytes > 0L) then total += bytes
          index += 1
        }
        total
      case None => -1L
