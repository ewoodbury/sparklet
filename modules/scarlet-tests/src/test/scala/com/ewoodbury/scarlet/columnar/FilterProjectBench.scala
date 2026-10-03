package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Times unfused Int32 filter (`value > 0`) and project (`value * 2`) against the row path.
 *
 * The timed region includes output allocation. Filter and project stay separate. Inputs are built
 * before the timer. The row collection uses one partition and opts out of columnar execution.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.columnar.FilterProjectBench 1000000"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object FilterProjectBench:

  private val defaultRows = 1000000
  private val defaultWarmup = 5
  private val defaultMeasure = 5

  private object Positive extends ColumnKernel.Int32Predicate:
    def apply(value: Int): Boolean = value > 0

  private object TimesTwo extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value * 2

  def main(args: Array[String]): Unit =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = arg(args, 0, defaultRows)
    val warmup = arg(args, 1, defaultWarmup)
    val measure = arg(args, 2, defaultMeasure)
    require(rows > 0 && warmup > 0 && measure > 0, "rows, warmup, and measure must be positive")
    require(measure % 2 == 1, "measure count must be odd so the median is a sample")

    val values = generate(rows)
    val expected = expectedResult(values)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32),
      Vector(Int32Column.of(values, Validity.allValid(rows), rows)),
      rows,
    )
    val rowsPath = DistCollection(values.toVector, 1).filter(_ > 0).map(_ * 2)

    checkColumnar(batch, expected)
    val rowSum = rowsPath.aggregate(0L)((sum, value) => sum + value, _ + _)
    require(rowSum == expected.sum && rowsPath.count() == expected.count.toLong, s"row sum $rowSum")

    (0 until warmup).foreach { _ =>
      runColumnar(batch)
      rowsPath.aggregate(0L)((sum, value) => sum + value, _ + _)
      ()
    }

    val columnarNs = new Array[Long](measure)
    val rowNs = new Array[Long](measure)
    (0 until measure).foreach { sample =>
      val columnarStart = System.nanoTime()
      val columnar = runColumnar(batch)
      val columnarElapsed = System.nanoTime() - columnarStart
      require(
        columnar.sum == expected.sum && columnar.count == expected.count,
        s"columnar sum ${columnar.sum}",
      )
      columnarNs(sample) = columnarElapsed

      val rowStart = System.nanoTime()
      val summed = rowsPath.aggregate(0L)((sum, value) => sum + value, _ + _)
      val rowElapsed = System.nanoTime() - rowStart
      require(summed == expected.sum, s"row sum $summed")
      rowNs(sample) = rowElapsed
    }

    val columnarMedian = median(columnarNs)
    val rowMedian = median(rowNs)
    println("filter > 0 then project * 2")
    println(s"rows=$rows partitions=1 warmup=$warmup measure=$measure")
    println(s"survivors=${expected.count} sum=${expected.sum}")
    println(s"columnar ${format(rows, columnarMedian)}")
    println(s"row      ${format(rows, rowMedian)}")

  private def runColumnar(batch: ColumnBatch): Result =
    val filtered = ColumnKernel.filterInt32(batch, 0, Positive)
    val projected = ColumnKernel.projectInt32(filtered, 0, TimesTwo)
    projected.columns match
      case Seq(column: Int32Column) =>
        val values = column.values
        val live = column.length
        var index = 0
        var sum = 0L
        while (index < live) {
          sum += values(index)
          index += 1
        }
        Result(sum, live)
      case _ =>
        throw new IllegalStateException(s"expected one Int32 column, got ${projected.schema}")

  private def checkColumnar(batch: ColumnBatch, expected: Result): Unit =
    val result = runColumnar(batch)
    require(
      result.sum == expected.sum && result.count == expected.count,
      s"columnar sum ${result.sum} count ${result.count}",
    )

  private def expectedResult(values: Array[Int]): Result =
    var index = 0
    var sum = 0L
    var count = 0
    while (index < values.length) {
      val value = values(index)
      if (value > 0) then
        sum += value.toLong * 2L
        count += 1
      index += 1
    }
    Result(sum, count)

  private def generate(rows: Int): Array[Int] =
    val values = new Array[Int](rows)
    var index = 0
    var state = 0x12345678
    while (index < rows) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    values

  private def median(samples: Array[Long]): Long =
    val sorted = samples.sorted
    sorted(sorted.length / 2)

  private def format(rows: Int, nanos: Long): String =
    require(nanos > 0L, "sample time must be positive")
    val millis = nanos / 1000000L
    val fraction = ((nanos / 1000L) % 1000L).toString
    val padded = "0" * (3 - fraction.length) + fraction
    val perSecond = (rows.toLong * 1000000000L) / nanos
    s"median $millis.$padded ms  $perSecond rows/s"

  private def arg(args: Array[String], index: Int, default: Int): Int =
    if (index < args.length) then args(index).toInt else default

  private final case class Result(sum: Long, count: Int)
