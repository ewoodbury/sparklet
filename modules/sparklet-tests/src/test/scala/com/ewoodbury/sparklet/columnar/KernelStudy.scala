package com.ewoodbury.sparklet.columnar

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Arrays

import com.sun.management.ThreadMXBean

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.core.SparkletConf

/**
 * One-shot profiles of the unfused Int32 kernels. Not a test, and not a paper figure.
 *
 * Each sample checks a scalar checksum. The timed region includes output allocation and the
 * checksum scan. Inputs are built beforehand. DistCollection samples opt out of columnar
 * execution so they stay on the row path. The machine is whatever the process is running on;
 * this laptop's CPU governor is often `powersave`, so absolute rows/s move around.
 *
 * {{{
 * sbt "sparklet-tests/Test/runMain com.ewoodbury.sparklet.columnar.KernelStudy"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object KernelStudy:

  private val warmup = 5
  private val measure = 11

  private object TimesTwo extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value * 2

  private object PlusOne extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value + 1

  private object HeavyMap extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value * 3 + value * 5 - 7

  private final class Greater(bound: Int) extends ColumnKernel.Int32Predicate:
    def apply(value: Int): Boolean = value > bound

  private object HeavyPred extends ColumnKernel.Int32Predicate:
    def apply(value: Int): Boolean = value * 3 + 1 > 0

  @volatile private var hold: ColumnBatch =
    new ColumnBatch(Vector.empty[LogicalType], Vector.empty[Column], 0)

  def main(args: Array[String]): Unit =
    SparkletConf.set(SparkletConf.get.copy(columnarExecution = false))
    val bean = threadBean
    bean.foreach(_.setThreadAllocatedMemoryEnabled(true))
    println(header(bean.isDefined))

    val rows1e6 = 1000000
    val data1e6 = generate(rows1e6)
    val batch1e6 = batchOf(data1e6, 0)
    val standard = oracleFilterProject(data1e6, bound = 0, nullPercent = 0)
    require(
      standard.sum == 500354792612L && standard.count == 500068,
      s"generator drifted: $standard",
    )
    require(
      kernelFilterProject(batch1e6, new Greater(0)) == standard.sum,
      "kernel disagrees with the scalar oracle",
    )

    profile(data1e6, batch1e6, standard)
    sizeSweep()
    selectivity(data1e6)
    nulls(data1e6)
    width(data1e6)
    morsels(data1e6)
    expressions(data1e6, batch1e6)
    require(hold.length >= 0, "published batch was lost")

  private def profile(data: Array[Int], batch: ColumnBatch, standard: Oracle): Unit =
    val rows = data.length
    val greater = new Greater(0)
    emit("fused-while", rows, standard.sum, standard.count) {
      fusedFilterProject(data, 0)
    }
    emit("primitive-unfused", rows, standard.sum, standard.count) {
      primitiveUnfused(data, 0)
    }
    emit("columnar-filter-project", rows, standard.sum, standard.count) {
      kernelFilterProject(batch, greater)
    }
    val filterOnly = oracleFilter(data, 0, 0)
    emit("columnar-filter", rows, filterOnly.sum, filterOnly.count) {
      val filtered = ColumnKernel.filterInt32(batch, 0, greater)
      hold = filtered
      sumPresent(filtered)
    }
    val projectOnly = oracleProject(data, 0)
    emit("columnar-project", rows, projectOnly.sum, projectOnly.count) {
      val projected = ColumnKernel.projectInt32(batch, 0, TimesTwo)
      hold = projected
      sumPresent(projected)
    }
    val read = oracleRead(data, 0)
    emit("read-sum", rows, read.sum, read.count) {
      sumArray(data)
    }
    val depth = oraclePlus(data, 4, 0)
    emit("columnar-project-x4", rows, depth.sum, depth.count) {
      projectChain(batch, 4)
    }
    emit("iterator-boxed", rows, standard.sum, standard.count) {
      data.iterator.filter(_ > 0).map(_ * 2).foldLeft(0L)((sum, value) => sum + value)
    }
    val rowsPath = DistCollection(data.toVector, 1).filter(_ > 0).map(_ * 2)
    emit("distcollection-1part", rows, standard.sum, standard.count) {
      rowsPath.aggregate(0L)((sum, value) => sum + value, (left, right) => left + right)
    }

  private def sizeSweep(): Unit =
    Array(1000, 10000, 100000, 1000000, 10000000, 100000000).foreach { rows =>
      val data = generate(rows)
      val batch = batchOf(data, 0)
      val expected = oracleFilterProject(data, 0, 0)
      val greater = new Greater(0)
      emit(s"fused-while-n$rows", rows, expected.sum, expected.count) {
        fusedFilterProject(data, 0)
      }
      emit(s"columnar-n$rows", rows, expected.sum, expected.count) {
        kernelFilterProject(batch, greater)
      }
      if (rows <= 10000000) then
        val rowsPath = DistCollection(data.toVector, 1).filter(_ > 0).map(_ * 2)
        emit(s"distcollection-n$rows", rows, expected.sum, expected.count) {
          rowsPath.aggregate(0L)((sum, value) => sum + value, (left, right) => left + right)
        }
    }

  private def selectivity(data: Array[Int]): Unit =
    Array(-1000001, -500000, 0, 500000, 900000, 1000000).foreach { bound =>
      val batch = batchOf(data, 0)
      val expected = oracleFilterProject(data, bound, 0)
      val greater = new Greater(bound)
      emit(s"select-bound$bound", data.length, expected.sum, expected.count) {
        kernelFilterProject(batch, greater)
      }
    }

  private def nulls(data: Array[Int]): Unit =
    Array(0, 10, 50, 90).foreach { percent =>
      val batch = batchOf(data, percent)
      val expected = oracleFilterProject(data, 0, percent)
      val greater = new Greater(0)
      emit(s"nulls-$percent", data.length, expected.sum, expected.count) {
        kernelFilterProject(batch, greater)
      }
    }

  private def width(data: Array[Int]): Unit =
    Array(1, 4, 16).foreach { columns =>
      val batch = wideBatch(data, columns)
      val expected = oracleFilter(data, 0, 0)
      val greater = new Greater(0)
      emit(s"width-$columns", data.length, expected.sum, expected.count) {
        val filtered = ColumnKernel.filterInt32(batch, 0, greater)
        hold = filtered
        sumPresent(filtered)
      }
    }

  private def morsels(data: Array[Int]): Unit =
    Array(1000, 10000, 100000, 1000000).foreach { batchSize =>
      val batches = chunks(data, batchSize)
      val expected = oracleFilterProject(data, 0, 0)
      val greater = new Greater(0)
      emit(s"morsel-$batchSize", data.length, expected.sum, expected.count) {
        var part = 0
        var sum = 0L
        while (part < batches.length) {
          sum += kernelFilterProject(batches(part), greater)
          part += 1
        }
        sum
      }
    }

  private def expressions(data: Array[Int], batch: ColumnBatch): Unit =
    val rows = data.length
    val heavy = oracleHeavy(data)
    emit("heavy-fused-while", rows, heavy.sum, heavy.count) {
      fusedHeavy(data)
    }
    emit("heavy-columnar", rows, heavy.sum, heavy.count) {
      val filtered = ColumnKernel.filterInt32(batch, 0, HeavyPred)
      val projected = ColumnKernel.projectInt32(filtered, 0, HeavyMap)
      hold = projected
      sumPresent(projected)
    }

  private def emit(name: String, rows: Int, checksum: Long, survivors: Int)(body: => Long): Unit =
    System.err.println(s"run $name")
    (0 until warmup).foreach { _ =>
      val got = body
      require(got == checksum, s"$name warmup checksum $got != $checksum")
    }
    val samples = new Array[Long](measure)
    val allocBefore = allocatedBytes
    val gcBefore = gcMillis
    (0 until measure).foreach { round =>
      val start = System.nanoTime()
      val got = body
      val elapsed = System.nanoTime() - start
      require(got == checksum, s"$name checksum $got != $checksum")
      samples(round) = elapsed
    }
    val allocDelta = allocatedBytes - allocBefore
    val allocPerCall = if (allocDelta >= 0L) allocDelta / measure.toLong else -1L
    val gc = gcMillis - gcBefore
    val p50 = percentile(samples, 50)
    val p90 = percentile(samples, 90)
    val min = percentile(samples, 0)
    val max = percentile(samples, 100)
    val rate = if (p50 <= 0L) 0L else (rows.toLong * 1000000000L) / p50
    println(
      s"$name\trows=$rows\tsurvivors=$survivors\tp50_ns=$p50\tmin_ns=$min\tp90_ns=$p90\tmax_ns=$max\trows_per_s=$rate\talloc_bytes=$allocPerCall\tgc_ms=$gc\tchecksum=$checksum",
    )

  private def kernelFilterProject(batch: ColumnBatch, predicate: ColumnKernel.Int32Predicate): Long =
    val filtered = ColumnKernel.filterInt32(batch, 0, predicate)
    val projected = ColumnKernel.projectInt32(filtered, 0, TimesTwo)
    hold = projected
    sumPresent(projected)

  private def projectChain(batch: ColumnBatch, depth: Int): Long =
    var current = batch
    var step = 0
    while (step < depth) {
      current = ColumnKernel.projectInt32(current, 0, PlusOne)
      step += 1
    }
    hold = current
    sumPresent(current)

  private def fusedFilterProject(values: Array[Int], bound: Int): Long =
    var index = 0
    var sum = 0L
    while (index < values.length) {
      val value = values(index)
      if (value > bound) then sum += value.toLong * 2L
      index += 1
    }
    sum

  private def fusedHeavy(values: Array[Int]): Long =
    var index = 0
    var sum = 0L
    while (index < values.length) {
      val value = values(index)
      if (value * 3 + 1 > 0) then sum += (value * 3 + value * 5 - 7).toLong
      index += 1
    }
    sum

  private def primitiveUnfused(values: Array[Int], bound: Int): Long =
    val length = values.length
    val filtered = new Array[Int](length)
    var index = 0
    var written = 0
    while (index < length) {
      val value = values(index)
      if (value > bound) then
        filtered(written) = value
        written += 1
      index += 1
    }
    val projected = new Array[Int](length)
    var out = 0
    while (out < written) {
      projected(out) = filtered(out) * 2
      out += 1
    }
    var sum = 0L
    var cursor = 0
    while (cursor < written) {
      sum += projected(cursor)
      cursor += 1
    }
    sum

  private def sumPresent(batch: ColumnBatch): Long =
    batch.columns.lift(0) match
      case Some(column: Int32Column) =>
        val words = column.validity.words
        val values = column.values
        val live = column.length
        var index = 0
        var sum = 0L
        while (index < live) {
          if (Validity.isSet(words, index)) then sum += values(index)
          index += 1
        }
        sum
      case _ =>
        throw new IllegalStateException(s"expected an Int32 column, got ${batch.schema}")

  private def sumArray(values: Array[Int]): Long =
    var index = 0
    var sum = 0L
    while (index < values.length) {
      sum += values(index)
      index += 1
    }
    sum

  private def batchOf(values: Array[Int], nullPercent: Int): ColumnBatch =
    val validity =
      if (nullPercent == 0) then Validity.allValid(values.length)
      else Validity.pack(values.length, row => present(row, nullPercent))
    new ColumnBatch(
      Vector(LogicalType.Int32),
      Vector(Int32Column.of(values, validity, values.length)),
      values.length,
    )

  private def wideBatch(values: Array[Int], columns: Int): ColumnBatch =
    val schema = Vector.fill(columns)(LogicalType.Int32)
    val built = Vector.tabulate(columns) { _ =>
      val copy = values.clone()
      Int32Column.of(copy, Validity.allValid(copy.length), copy.length)
    }
    new ColumnBatch(schema, built, values.length)

  private def chunks(values: Array[Int], batchSize: Int): Array[ColumnBatch] =
    val count = (values.length + batchSize - 1) / batchSize
    val batches = new Array[ColumnBatch](count)
    var part = 0
    var start = 0
    while (part < count) {
      val end = math.min(start + batchSize, values.length)
      val slice = Arrays.copyOfRange(values, start, end)
      batches(part) = batchOf(slice, 0)
      start = end
      part += 1
    }
    batches

  private def oracleFilterProject(values: Array[Int], bound: Int, nullPercent: Int): Oracle =
    var index = 0
    var sum = 0L
    var count = 0
    while (index < values.length) {
      if (present(index, nullPercent) && values(index) > bound) then
        sum += values(index).toLong * 2L
        count += 1
      index += 1
    }
    Oracle(sum, count)

  private def oracleFilter(values: Array[Int], bound: Int, nullPercent: Int): Oracle =
    var index = 0
    var sum = 0L
    var count = 0
    while (index < values.length) {
      if (present(index, nullPercent) && values(index) > bound) then
        sum += values(index)
        count += 1
      index += 1
    }
    Oracle(sum, count)

  private def oracleProject(values: Array[Int], nullPercent: Int): Oracle =
    var index = 0
    var sum = 0L
    var count = 0
    while (index < values.length) {
      if (present(index, nullPercent)) then
        sum += values(index).toLong * 2L
        count += 1
      index += 1
    }
    Oracle(sum, count)

  private def oracleRead(values: Array[Int], nullPercent: Int): Oracle =
    var index = 0
    var sum = 0L
    var count = 0
    while (index < values.length) {
      if (present(index, nullPercent)) then
        sum += values(index)
        count += 1
      index += 1
    }
    Oracle(sum, count)

  private def oraclePlus(values: Array[Int], delta: Int, nullPercent: Int): Oracle =
    var index = 0
    var sum = 0L
    var count = 0
    while (index < values.length) {
      if (present(index, nullPercent)) then
        sum += values(index).toLong + delta.toLong
        count += 1
      index += 1
    }
    Oracle(sum, count)

  private def oracleHeavy(values: Array[Int]): Oracle =
    var index = 0
    var sum = 0L
    var count = 0
    while (index < values.length) {
      val value = values(index)
      if (value * 3 + 1 > 0) then
        sum += (value * 3 + value * 5 - 7).toLong
        count += 1
      index += 1
    }
    Oracle(sum, count)

  private def present(row: Int, nullPercent: Int): Boolean =
    nullPercent == 0 || ((row * 17) % 100) >= nullPercent

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

  private def percentile(samples: Array[Long], percent: Int): Long =
    val sorted = samples.clone()
    Arrays.sort(sorted)
    val index =
      if (percent <= 0) then 0
      else if (percent >= 100) then sorted.length - 1
      else (percent * (sorted.length - 1)) / 100
    sorted(index)

  private def header(alloc: Boolean): String =
    val runtime = ManagementFactory.getRuntimeMXBean
    val model = cpuModel
    val governor = readFirst("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    val total = readFirstLine("/proc/meminfo")
    s"java=${runtime.getVmName} ${runtime.getVmVersion} args=${runtime.getInputArguments}\ncpu=$model governor=$governor $total\nalloc_bytes=${if (alloc) "all-threads" else "unavailable"} warmup=$warmup measure=$measure\nname rows survivors p50_ns min_ns p90_ns max_ns rows_per_s alloc_bytes gc_ms checksum"

  private def cpuModel: String =
    readText("/proc/cpuinfo").linesIterator.find(_.startsWith("model name")).getOrElse("unknown")

  private def readFirst(path: String): String =
    readText(path).linesIterator.next()

  private def readFirstLine(path: String): String =
    readFirst(path)

  private def readText(path: String): String =
    Files.readString(Path.of(path), StandardCharsets.UTF_8)

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

  private def gcMillis: Long =
    val beans = ManagementFactory.getGarbageCollectorMXBeans
    var index = 0
    var total = 0L
    while (index < beans.size) {
      total += beans.get(index).getCollectionTime
      index += 1
    }
    total

  private final case class Oracle(sum: Long, count: Int)
