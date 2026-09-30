package com.ewoodbury.sparklet.columnar

import java.lang.management.ManagementFactory

import com.sun.management.ThreadMXBean

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.core.SparkletConf

/**
 * Profiles the inner hash join. Not a test. The timed region includes the build table and the
 * output batch. Inputs are built beforehand. The DistCollection join sample opts out of columnar
 * execution.
 *
 * {{{
 * sbt "sparklet-tests/Test/runMain com.ewoodbury.sparklet.columnar.JoinStudy"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object JoinStudy:

  private val warmup = 3
  private val measure = 5

  def main(args: Array[String]): Unit =
    SparkletConf.set(SparkletConf.get.copy(columnarExecution = false))
    threadBean.foreach(_.setThreadAllocatedMemoryEnabled(true))
    println("hash join study warmup=3 measure=5")
    kernel(buildRows = 50000, probeRows = 50000, matchEvery = 1)
    kernel(buildRows = 100000, probeRows = 1000000, matchEvery = 1)
    kernel(buildRows = 100000, probeRows = 1000000, matchEvery = 10)
    row(buildRows = 50000, probeRows = 50000)

  private def kernel(buildRows: Int, probeRows: Int, matchEvery: Int): Unit =
    val build = side(buildRows, buildRows)
    val probe = side(probeRows, buildRows * matchEvery)
    val expected = probeRows.toLong / matchEvery.toLong
    emit(s"join-build$buildRows-probe$probeRows-every$matchEvery", probeRows, expected) {
      val joined = HashJoin.innerInt32(
        HashJoin.JoinSide(build, key = 0, value = 1),
        HashJoin.JoinSide(probe, key = 0, value = 1),
      )
      joined.length.toLong
    }

  private def row(buildRows: Int, probeRows: Int): Unit =
    val buildPairs = (0 until buildRows).map { row => (row, row) }.toVector
    val probePairs = (0 until probeRows).map { row => (row % buildRows, row) }.toVector
    val plan = DistCollection(buildPairs, 4).join(DistCollection(probePairs, 4))
    emit(s"rowjoin-build$buildRows-probe$probeRows", probeRows, probeRows.toLong) {
      plan.count()
    }

  private def side(rows: Int, keySpace: Int): ColumnBatch =
    val keys = new Array[Int](rows)
    val values = new Array[Int](rows)
    var index = 0
    while (index < rows) {
      keys(index) = if (keySpace == rows) index else index % keySpace
      values(index) = index
      index += 1
    }
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.allValid(rows), rows),
        Int32Column.of(values, Validity.allValid(rows), rows),
      ),
      rows,
    )

  private def emit(name: String, rows: Int, expected: Long)(body: => Long): Unit =
    System.err.println(s"run $name")
    (0 until warmup).foreach { _ =>
      val got = body
      require(got == expected, s"$name warmup $got != $expected")
    }
    val samples = new Array[Long](measure)
    val before = allocatedBytes
    (0 until measure).foreach { sample =>
      val start = System.nanoTime()
      val got = body
      val elapsed = System.nanoTime() - start
      require(got == expected, s"$name $got != $expected")
      samples(sample) = elapsed
    }
    val sorted = samples.clone()
    java.util.Arrays.sort(sorted)
    val p50 = sorted(sorted.length / 2)
    val alloc = (allocatedBytes - before) / measure.toLong
    val rate = if (p50 <= 0L) 0L else (rows.toLong * 1000000000L) / p50
    println(s"$name\trows=$rows\tp50_ns=$p50\trows_per_s=$rate\talloc_bytes=$alloc\tmatches=$expected")

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
