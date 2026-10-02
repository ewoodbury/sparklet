package com.ewoodbury.sparklet.execution

import java.lang.management.ManagementFactory

import com.sun.management.ThreadMXBean

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.columnar.*
import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf}
import com.ewoodbury.sparklet.runtime.SparkletRuntime

/**
 * Times `collect()` with columnar execution on and off. Not a test.
 *
 * The timed region is `collect()`: encode, the kernels or the row plan, and the boxed decode.
 * The allocation sample is inside that region. The checksum is outside it. Narrow is
 * `filter(_ > 0).map(_ * 2)` over 1e6 ints. `reduceByKey` is measured at 1024 groups and at one
 * group per row, and those two also print the columnar stages under the collect line: encode,
 * morsel partial, per-batch merge, exchange, final reduce, decode. The stage sum is the kernel
 * pipeline, not `collect()`. The join is 50k by 50k. A million-by-million join is not run.
 *
 * {{{
 * sbt "sparklet-tests/Test/runMain com.ewoodbury.sparklet.execution.DistCollectionStudy"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object DistCollectionStudy:

  private val warmup = 3
  private val measure = 5
  private val rows = 1000000
  private val groups = 1024
  private val joinRows = 50000
  private val partitions = 4
  private val stageNames = Vector("encode", "partial", "merge", "exchange", "final", "decode")

  def main(args: Array[String]): Unit =
    val saved = SparkletConf.get
    try
      threadBean.foreach(_.setThreadAllocatedMemoryEnabled(true))
      println(
        s"distcollection study warmup=$warmup measure=$measure threads=${saved.threadPoolSize} " +
          s"batch=${saved.columnarBatchSize} shuffle=${saved.defaultShufflePartitions}",
      )
      val values = generate(rows)
      val (narrowSum, narrowCount) = expectNarrow(values)
      val narrowPlan = DistCollection(values.toVector, partitions).filter(_ > 0).map(_ * 2)
      timeCollect("narrow", rows, narrowSum, narrowPlan) { collected =>
        require(collected.size == narrowCount, s"narrow rows ${collected.size}")
        sumInts(collected)
      }

      val reducePlan = DistCollection(Vector.tabulate(rows)(row => (row % groups, row)), partitions)
        .reduceByKey[Int, Int](_ + _)
      val reduceSum = (rows.toLong - 1L) * rows.toLong / 2L
      timeCollect("reduce1024", rows, reduceSum, reducePlan) { collected =>
        require(collected.size == groups, s"reduce groups ${collected.size}")
        sumReduced(collected)
      }
      timeReduceStages("reduce1024", reducePlan, reduceSum, groups)

      val distinctPlan = DistCollection(Vector.tabulate(rows)(row => (row, 1)), partitions)
        .reduceByKey[Int, Int](_ + _)
      timeCollect("reduce1e6", rows, rows.toLong, distinctPlan) { collected =>
        require(collected.size == rows, s"reduce groups ${collected.size}")
        sumReduced(collected)
      }
      timeReduceStages("reduce1e6", distinctPlan, rows.toLong, rows)

      val side = Vector.tabulate(joinRows)(row => (row, row))
      val joinPlan = DistCollection(side, partitions).join(DistCollection(side, partitions))
      val joinSum = (joinRows.toLong - 1L) * joinRows.toLong / 2L
      timeCollect("join50k", joinRows, joinSum, joinPlan) { collected =>
        require(collected.size == joinRows, s"join rows ${collected.size}")
        sumJoinKeys(collected)
      }
    finally SparkletConf.set(saved)

  private def timeCollect[A](
      name: String,
      inputRows: Int,
      expected: Long,
      plan: DistCollection[A],
  )(checksum: Iterable[A] => Long): Unit =
    Seq(false, true).foreach { flag =>
      SparkletConf.set(SparkletConf.get.copy(columnarExecution = flag))
      val label = if (flag) "columnar" else "row"
      val title = s"$name-$label"
      System.err.println(s"run $title")
      (0 until warmup).foreach { _ =>
        SparkletRuntime.get.shuffle.clear()
        val got = checksum(plan.collect())
        require(got == expected, s"$title warmup $got != $expected")
      }
      val samples = new Array[Long](measure)
      var allocated = 0L
      (0 until measure).foreach { sample =>
        SparkletRuntime.get.shuffle.clear()
        val (collected, elapsed, bytes) = timed(plan.collect())
        allocated += bytes
        val got = checksum(collected)
        require(got == expected, s"$title $got != $expected")
        samples(sample) = elapsed
      }
      val sorted = samples.clone()
      java.util.Arrays.sort(sorted)
      val p50 = sorted(measure / 2)
      val alloc = allocated / measure.toLong
      val rate = if (p50 <= 0L) 0L else (inputRows.toLong * 1000000000L) / p50
      println(s"$title\trows=$inputRows\tp50_ns=$p50\trows_per_s=$rate\talloc_bytes=$alloc")
    }

  /** Columnar reduce stages on the plan's source partitions. Not `collect()`. */
  private def timeReduceStages(
      name: String,
      plan: DistCollection[(Int, Int)],
      expected: Long,
      groupCount: Int,
  ): Unit =
    val conf = SparkletConf.get
    val batchSize = conf.columnarBatchSize
    val parallelism = conf.threadPoolSize
    val width = conf.defaultShufflePartitions
    val parts = sourcePairs(plan.plan)
    val op: HashReduce.IntBinOp = (left, right) => left + right
    System.err.println(s"run $name-stages")
    def run(): (Vector[Seq[(Int, Int)]], Array[Long], Array[Long]) =
      val (encoded, encodeNs, encodeBytes) = timed {
        parts.map(part => BatchCodec.intPairs(part.data)).toVector
      }
      val ((sizes, partials), partialNs, partialBytes) = timed {
        PartialCombine.partialMorsels(
          encoded,
          batchSize,
          parallelism,
          morsel => HashReduce.int32(morsel, keyOrdinal = 0, valueOrdinal = 1, op),
        )
      }
      val (merged, mergeNs, mergeBytes) = timed {
        PartialCombine.mergeMorsels(
          sizes,
          partials,
          parallelism,
          batch => HashReduce.int32(batch, keyOrdinal = 0, valueOrdinal = 1, op),
        )
      }
      val (buckets, exchangeNs, exchangeBytes) = timed {
        HashExchange.partitionAll(merged, keyOrdinal = 0, numPartitions = width, parallelism)
      }
      val (reduced, finalNs, finalBytes) = timed {
        MorselScheduler.map(buckets, parallelism) { (_, bucket) =>
          HashReduce.int32(bucket, keyOrdinal = 0, valueOrdinal = 1, op)
        }
      }
      val (decoded, decodeNs, decodeBytes) = timed {
        reduced.map(batch => BatchCodec.decodeIntPairs(batch))
      }
      val elapsed = Array(encodeNs, partialNs, mergeNs, exchangeNs, finalNs, decodeNs)
      val bytes = Array(
        encodeBytes,
        partialBytes,
        mergeBytes,
        exchangeBytes,
        finalBytes,
        decodeBytes,
      )
      (decoded, elapsed, bytes)
    def check(decoded: Vector[Seq[(Int, Int)]], label: String): Unit =
      val (got, count) = sumDecoded(decoded)
      require(got == expected, s"$label $got != $expected")
      require(count == groupCount, s"$label groups $count")
    (0 until warmup).foreach { index =>
      val (decoded, _, _) = run()
      check(decoded, s"$name-stages warmup $index")
    }
    val samples = Array.ofDim[Long](stageNames.length, measure)
    val allocated = Array.ofDim[Long](stageNames.length, measure)
    val totals = new Array[Long](measure)
    (0 until measure).foreach { sample =>
      val (decoded, elapsed, bytes) = run()
      check(decoded, s"$name-stages")
      var stage = 0
      var total = 0L
      while (stage < elapsed.length) {
        samples(stage)(sample) = elapsed(stage)
        allocated(stage)(sample) = bytes(stage)
        total += elapsed(stage)
        stage += 1
      }
      totals(sample) = total
    }
    stageNames.zipWithIndex.foreach { (label, stage) =>
      val column = Array.tabulate(measure)(sample => samples(stage)(sample))
      val bytes = Array.tabulate(measure)(sample => allocated(stage)(sample))
      println(s"$name-$label\tp50_ns=${median(column)}\talloc_bytes=${mean(bytes)}")
    }
    println(s"$name-kernel\tp50_ns=${median(totals)}")

  private def sourcePairs(plan: Plan[(Int, Int)]): Seq[Partition[(Int, Int)]] = plan match
    case Plan.ReduceByKeyOp(Plan.Source(parts), _) => parts
    case _ => throw new IllegalArgumentException("stage timing expects reduceByKey of a source")

  private def sumDecoded(batches: Vector[Seq[(Int, Int)]]): (Long, Int) =
    var sum = 0L
    var count = 0
    val batchesIterator = batches.iterator
    while (batchesIterator.hasNext) {
      val pairs = batchesIterator.next().iterator
      while (pairs.hasNext) {
        sum += pairs.next()._2.toLong
        count += 1
      }
    }
    (sum, count)

  private def timed[A](body: => A): (A, Long, Long) =
    val before = allocatedBytes
    val start = System.nanoTime()
    val value = body
    val after = allocatedBytes
    (value, System.nanoTime() - start, after - before)

  private def median(values: Array[Long]): Long =
    val sorted = values.clone()
    java.util.Arrays.sort(sorted)
    sorted(values.length / 2)

  private def mean(values: Array[Long]): Long =
    var index = 0
    var total = 0L
    while (index < values.length) {
      total += values(index)
      index += 1
    }
    total / values.length.toLong

  private def sumInts(values: Iterable[Int]): Long =
    var sum = 0L
    val iterator = values.iterator
    while (iterator.hasNext) sum += iterator.next().toLong
    sum

  private def sumReduced(values: Iterable[(Int, Int)]): Long =
    var sum = 0L
    val iterator = values.iterator
    while (iterator.hasNext) sum += iterator.next()._2.toLong
    sum

  private def sumJoinKeys(values: Iterable[(Int, (Int, Int))]): Long =
    var sum = 0L
    val iterator = values.iterator
    while (iterator.hasNext) sum += iterator.next()._1.toLong
    sum

  private def expectNarrow(values: Array[Int]): (Long, Int) =
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
    (sum, count)

  private def generate(n: Int): Array[Int] =
    val values = new Array[Int](n)
    var index = 0
    var state = 0x12345678
    while (index < n) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    values

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
