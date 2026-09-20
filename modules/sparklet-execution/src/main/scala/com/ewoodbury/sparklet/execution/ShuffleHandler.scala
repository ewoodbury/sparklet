package com.ewoodbury.sparklet.execution

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.{Partition, ShuffleId, SparkletConf, StageId}
import com.ewoodbury.sparklet.runtime.api.{Partitioner, ShuffleService}

/**
 * Handler for managing shuffle operations.
 */
final class ShuffleHandler[F[_]: Sync](
    shuffle: ShuffleService,
    partitioner: Partitioner,
) extends StrictLogging:

  /**
   * Handles shuffle output with improved type safety. The cast is necessary as the results from
   * `executeStage` are untyped, but we centralize it here with clear documentation.
   */
  @SuppressWarnings(
    Array("org.wartremover.warts.MutableDataStructures", "org.wartremover.warts.AsInstanceOf"),
  )
  def handleShuffleOutput(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
  ): F[ShuffleId] =
    handleKeyedOutput(stageInfo, results, SparkletConf.get.defaultShufflePartitions)

  /**
   * Writes key-value results to the shuffle service, hash-partitioned by key into `numPartitions`
   * partitions. Used when downstream stages assume key partitioning.
   */
  @SuppressWarnings(
    Array("org.wartremover.warts.MutableDataStructures", "org.wartremover.warts.AsInstanceOf"),
  )
  def handleKeyedOutput(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
      numPartitions: Int,
  ): F[ShuffleId] =
    Sync[F].delay {
      val keyValueResults = results.asInstanceOf[Seq[Partition[(Any, Any)]]]
      val shuffleData = shuffle.partitionByKey[Any, Any](
        data = keyValueResults,
        numPartitions = numPartitions,
        partitioner = partitioner,
      )
      val actualShuffleId = shuffle.write[Any, Any](shuffleData)
      logger.debug(
        s"Stored shuffle data for stage ${stageInfo.id.toInt} with shuffle ID ${actualShuffleId.toInt}",
      )
      actualShuffleId
    }

  /**
   * Handles shuffle output for sortBy operations by mapping each data element of type T to a
   * key-value pair of type (T, Unit).
   */
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  def handleSortByRangePartitionedOutput(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
      stageGraph: StageBuilder.StageGraph,
      dependentSortByStageId: StageId,
  ): F[ShuffleId] =
    Sync[F].delay {
      stageGraph.stages(dependentSortByStageId).wideOp match {
        case Some(op @ SortByWideOp(meta)) =>
          handleSortByRangePartitionedOutputTyped(
            stageInfo,
            results,
            meta.numPartitions,
            op,
          )
        case _ =>
          throw new IllegalStateException(
            s"Stage ${dependentSortByStageId.toInt} is not a sortBy stage",
          )
      }
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.SeqApply",
      "org.wartremover.warts.MutableDataStructures",
    ),
  )
  private def handleSortByRangePartitionedOutputTyped[A, S](
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
      numPartitions: Int,
      sortBy: SortByWideOp[A, S],
  ): ShuffleId = {
    val ordering: Ordering[S] = sortBy.meta.ordering
    val expectedN = numPartitions
    given Ordering[S] = ordering

    val elements: Seq[A] = results.flatMap(partition => partition.data.asInstanceOf[Iterable[A]])
    val keys: Seq[S] = elements.map(sortBy.meta.keyFunc)

    val sample = takeKeySample(keys)
    val cutPoints = computeCutPoints(sample, expectedN)

    // Range partitioner using binary search over cut points
    val rangePartitioner = new Partitioner {
      def partition(key: Any, numPartitions: Int): Int = {
        if (numPartitions <= 1 || cutPoints.isEmpty) return 0

        // The key should be of type S, but Any is required by Partitioner interface
        val k = key.asInstanceOf[S]

        @annotation.tailrec
        def binarySearch(lo: Int, hi: Int, ans: Int): Int = {
          if (lo > hi) ans
          else {
            val mid = (lo + hi) >>> 1
            val cmp = ordering.compare(k, cutPoints(mid))
            if (cmp <= 0) binarySearch(lo, mid - 1, mid)
            else binarySearch(mid + 1, hi, ans)
          }
        }

        val result = binarySearch(0, cutPoints.length - 1, cutPoints.length)
        math.min(result, numPartitions - 1)
      }
    }

    // Form (key, value) pairs per input partition
    val kvPartitions: Seq[Partition[(S, A)]] =
      results.asInstanceOf[Seq[Partition[A]]].map { partition =>
        val it = partition.data.iterator
        val buf = scala.collection.mutable.ArrayBuffer.empty[(S, A)]
        while (it.hasNext) {
          val a = it.next()
          buf += ((sortBy.meta.keyFunc(a), a))
        }
        Partition(buf.toSeq)
      }

    val shuffleData: ShuffleService.ShuffleData[S, A] =
      shuffle.partitionByKey[S, A](kvPartitions, expectedN, rangePartitioner)

    val actualShuffleId = shuffle.write[S, A](shuffleData)
    logger.debug(
      s"Stored range-partitioned sortBy shuffle data for stage ${stageInfo.id.toInt} with shuffle ID ${actualShuffleId.toInt} across ${expectedN} partitions",
    )
    actualShuffleId
  }

  // --- Sampling utilities for sortBy ---
  private def takeKeySample[S](keys: Seq[S]): Seq[S] = {
    val perPart = SparkletConf.get.sortSamplePerPartition
    val maxSample = SparkletConf.get.sortMaxSample
    if (keys.isEmpty) Seq.empty
    else {
      val step = math.max(1, keys.size / math.max(1, SparkletConf.get.defaultShufflePartitions))
      val raw = keys.grouped(step).flatMap(_.take(perPart)).toSeq
      if (raw.size <= maxSample) raw else raw.take(maxSample)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.SeqApply"))
  private def computeCutPoints[S](sample: Seq[S], numPartitions: Int)(using
      ord: Ordering[S],
  ): Vector[S] = {
    if (numPartitions <= 1 || sample.isEmpty) Vector.empty[S]
    else {
      val sorted = sample.sorted
      val P = numPartitions
      val cutCount = math.max(0, P - 1)
      val step = sorted.size.toDouble / P
      (1 to cutCount).map { i =>
        val idx = math.min(sorted.size - 1, math.max(0, math.ceil(i * step).toInt - 1))
        sorted(idx) // idx is clamped to the sample bounds
      }.toVector
    }
  }

  /**
   * Handles shuffle output for repartition and coalesce operations by keying each element with a
   * Unit value and delegating to the shuffle service's partitioner.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.MutableDataStructures",
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
    ),
  )
  def handleRepartitionOrCoalesceOutput(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
      targetNumPartitions: Int,
  ): F[ShuffleId] =
    Sync[F].delay {
      val allData: Seq[Any] = results.flatMap(_.data)
      val keyedData: Seq[(Any, Unit)] = allData.map(element => (element, ()))
      val shuffleData = shuffle.partitionByKey[Any, Unit](
        data = Seq(Partition(keyedData)),
        numPartitions = targetNumPartitions,
        partitioner = partitioner,
      )
      val actualShuffleId = shuffle.write[Any, Unit](shuffleData)
      logger.debug(
        s"Stored repartition/coalesce shuffle data for stage ${stageInfo.id.toInt} with shuffle ID ${actualShuffleId.toInt}",
      )
      actualShuffleId
    }
