package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Times string `collect()` with columnar execution on and off. Not a test.
 *
 * Distinct runs use one string per row. The 1024 runs repeat `row % 1024`. Filter keeps a last
 * digit below `'5'`. Map appends `x`. The timed region is `collect()`. The checksum is outside it.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.StringStudy"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object StringStudy:

  private val warmup = 3
  private val measure = 5
  private val rows = 1000000
  private val groups = 1024
  private val partitions = 4

  def main(args: Array[String]): Unit =
    val saved = ScarletConf.get
    try
      threadBean.foreach(_.setThreadAllocatedMemoryEnabled(true))
      println(
        s"string study warmup=$warmup measure=$measure threads=${saved.threadPoolSize} " +
          s"batch=${saved.columnarBatchSize} partitions=$partitions",
      )
      locally {
        val distinct = Vector.tabulate(rows)(_.toString)
        runFilter("filter-distinct", distinct)
        runMap("map-distinct", distinct)
      }
      locally {
        val repeated = Vector.tabulate(rows)(row => (row % groups).toString)
        runFilter("filter-1024", repeated)
        runMap("map-1024", repeated)
      }
    finally ScarletConf.set(saved)

  private def runFilter(name: String, values: Vector[String]): Unit =
    val plan = DistCollection(values, partitions).filter(keep)
    val (expected, count) = oracle(values, keep, text => text)
    timeCollect(name, rows, expected, count, plan)

  private def runMap(name: String, values: Vector[String]): Unit =
    val plan = DistCollection(values, partitions).map(suffix)
    val (expected, count) = oracle(values, _ => true, suffix)
    timeCollect(name, rows, expected, count, plan)

  private def keep(text: String): Boolean = text.charAt(text.length - 1) < '5'

  private def suffix(text: String): String = text.concat("x")

  private def timeCollect(
      name: String,
      inputRows: Int,
      expected: Long,
      expectedCount: Int,
      plan: DistCollection[String],
  ): Unit =
    Seq(false, true).foreach { flag =>
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = flag))
      val label = if (flag) "columnar" else "row"
      val title = s"$name-$label"
      System.err.println(s"run $title")
      (0 until warmup).foreach { _ =>
        val (got, count) = checksum(plan.collect())
        require(got == expected && count == expectedCount, s"$title warmup $got count $count")
      }
      val samples = new Array[Long](measure)
      var allocated = 0L
      (0 until measure).foreach { sample =>
        val (collected, elapsed, bytes) = timed(plan.collect())
        allocated += bytes
        val (got, count) = checksum(collected)
        require(got == expected && count == expectedCount, s"$title $got count $count")
        samples(sample) = elapsed
      }
      val sorted = samples.clone()
      java.util.Arrays.sort(sorted)
      val p50 = sorted(measure / 2)
      val alloc = allocated / measure.toLong
      val rate = if (p50 <= 0L) 0L else (inputRows.toLong * 1000000000L) / p50
      println(s"$title\trows=$inputRows\tp50_ns=$p50\trows_per_s=$rate\talloc_bytes=$alloc")
    }

  private def oracle(
      values: Vector[String],
      predicate: String => Boolean,
      mapper: String => String,
  ): (Long, Int) =
    var sum = 0L
    var count = 0
    val iterator = values.iterator
    while (iterator.hasNext) {
      val text = iterator.next()
      if (predicate(text)) then
        sum = mix(sum, mapper(text))
        count += 1
    }
    (sum, count)

  private def checksum(values: Iterable[String]): (Long, Int) =
    var sum = 0L
    var count = 0
    val iterator = values.iterator
    while (iterator.hasNext) {
      sum = mix(sum, iterator.next())
      count += 1
    }
    (sum, count)

  private def mix(sum: Long, text: String): Long = sum * 131L + text.hashCode

  private def timed[A](body: => A): (A, Long, Long) =
    val before = allocatedBytes
    val start = System.nanoTime()
    val value = body
    val after = allocatedBytes
    (value, System.nanoTime() - start, after - before)

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
