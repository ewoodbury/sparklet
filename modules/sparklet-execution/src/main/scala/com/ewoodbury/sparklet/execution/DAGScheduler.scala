package com.ewoodbury.sparklet.execution

import scala.collection.mutable

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.*
import com.ewoodbury.sparklet.runtime.api.{
  Partitioner,
  ShuffleService,
  SparkletRuntime,
  TaskScheduler,
}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.MutableDataStructures",
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.Var",
  ),
)
final class DAGScheduler[F[_]: Sync](
    shuffle: ShuffleService,
    scheduler: TaskScheduler[F],
    partitioner: Partitioner,
) extends StrictLogging:

  /**
   * Executes a plan using multi-stage execution, handling shuffle boundaries.
   */
  def execute[A](plan: Plan[A]): F[Iterable[A]] =
    for {
      _ <- Sync[F].delay(logger.info("DAGScheduler: starting multi-stage execution"))
      stageGraph <- Sync[F].delay(StageBuilder.buildStageGraph(plan))
      _ <- Sync[F].delay(
        logger.debug(s"DAGScheduler: built stage graph with ${stageGraph.stages.size} stages"),
      )
      executionOrder <- Sync[F].delay(DAGScheduler.topologicalSort(stageGraph.dependencies))
      _ <- Sync[F].delay(
        logger.debug(
          s"DAGScheduler: execution order: ${executionOrder.map(_.toInt).mkString(" -> ")}",
        ),
      )
      stageResults <- runStages(stageGraph, executionOrder)
      finalData <- Sync[F].delay {
        val finalResults = stageResults(stageGraph.finalStageId)
        finalResults.flatMap(_.data.asInstanceOf[Iterable[A]])
      }
      _ <- Sync[F].delay(logger.info("DAGScheduler: multi-stage execution completed"))
    } yield finalData

  /**
   * Gets input partitions for a stage, returning them with a wildcard type `_`. This avoids
   * casting at the source, deferring it to the execution function.
   */
  private def getInputPartitionsForStage(
      stageInfo: StageBuilder.StageInfo,
      stageResults: mutable.Map[StageId, Seq[Partition[_]]],
      stageToShuffleId: mutable.Map[StageId, ShuffleId],
  ): Seq[Partition[_]] = {
    stageInfo.inputSources.flatMap {
      case StageBuilder.SourceInput(partitions) =>
        partitions // No cast needed

      case StageBuilder.StageOutput(depStageId) =>
        // Read the already computed output of the dependent stage
        stageResults.getOrElse(depStageId, Seq.empty[Partition[_]])

      case StageBuilder.ShuffleFrom(upstreamStageId, numPartitions) =>
        val actualShuffleId = stageToShuffleId.getOrElse(
          upstreamStageId,
          throw new IllegalStateException(
            s"Missing shuffle id for upstream stage ${upstreamStageId.toInt} feeding stage ${stageInfo.id.toInt}",
          ),
        )
        (0 until numPartitions).map { partitionIdx =>
          shuffle.readPartition[Any, Any](actualShuffleId, PartitionId(partitionIdx))
        }

      case StageBuilder.TaggedShuffleFrom(upstreamStageId, _side, numPartitions) =>
        // Explicitly resolve upstream shuffle ID and read exactly those partitions.
        // Used for join and cogroup operations.
        val actualShuffleId = stageToShuffleId.getOrElse(
          upstreamStageId,
          throw new IllegalStateException(
            s"Missing shuffle id for upstream stage ${upstreamStageId.toInt} feeding stage ${stageInfo.id.toInt}",
          ),
        )
        (0 until numPartitions).map { partitionIdx =>
          shuffle.readPartition[Any, Any](actualShuffleId, PartitionId(partitionIdx))
        }
    }
  }

  /**
   * Executes a single stage, dispatching to a narrow or shuffle implementation. This function now
   * handles the necessary casting based on the stage type.
   */
  private def executeStage(
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[_]],
      stageToShuffleId: mutable.Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    if (inputPartitions.isEmpty) then
      Sync[F].delay(logger.warn(s"Stage ${stageInfo.id.toInt} has no input partitions")) *> Sync[F]
        .pure(Seq.empty)
    else {
      stageInfo match {
        case info if info.isShuffleStage =>
          val partitions = inputPartitions.asInstanceOf[Seq[Partition[(Any, Any)]]]
          executeShuffleStage(info, partitions, stageToShuffleId)
        case info =>
          val anyPartitions = inputPartitions.asInstanceOf[Seq[Partition[Any]]]
          executeNarrowStage(info, anyPartitions, stageToShuffleId)
      }
    }
  }

  /**
   * Iterates through the stages in topological order, executing each and materializing shuffle
   * outputs when needed. Returns a map of stage results.
   */
  private def runStages(
      stageGraph: StageBuilder.StageGraph,
      executionOrder: List[StageId],
  ): F[mutable.Map[StageId, Seq[Partition[_]]]] = {
    val stageResults = mutable.Map[StageId, Seq[Partition[_]]]()
    val stageToShuffleId = mutable.Map[StageId, ShuffleId]()

    executionOrder
      .foldLeftM(()) { (_, stageId) =>
        val stageInfo = stageGraph.stages(stageId)
        for {
          _ <- Sync[F].delay(logger.info(s"DAGScheduler: executing stage ${stageId.toInt}"))
          inputPartitions <- Sync[F].delay(
            getInputPartitionsForStage(stageInfo, stageResults, stageToShuffleId),
          )
          results <- executeStage(stageInfo, inputPartitions, stageToShuffleId)
          _ <- Sync[F].delay { stageResults(stageId) = results }
          _ <- writeShuffleIfNeeded(stageInfo, results, stageGraph, stageToShuffleId)
        } yield ()
      }
      .as(stageResults)
  }

  /**
   * If any dependent stage is a shuffle stage, persist this stage's output to the shuffle service.
   * For sortBy dependencies we use a special keying strategy to preserve element order.
   */
  private def writeShuffleIfNeeded(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
      stageGraph: StageBuilder.StageGraph,
      stageToShuffleId: mutable.Map[StageId, ShuffleId],
  ): F[Unit] = {
    val dependentStages: Iterable[StageId] =
      stageGraph.dependencies.filter(_._2.contains(stageInfo.id)).keys

    val needsShuffleOutput =
      dependentStages.exists(depStageId => stageGraph.stages(depStageId).isShuffleStage)

    if (!needsShuffleOutput) Sync[F].unit
    else {
      val hasSortByDependent = dependentStages.exists(depStageId =>
        stageGraph
          .stages(depStageId)
          .shuffleOperation
          .exists(_.isInstanceOf[Plan.SortByOp[_, _]]),
      )

      val repartitionDep = dependentStages
        .flatMap(id => stageGraph.stages(id).shuffleOperation.toSeq)
        .collectFirst { case op: Plan.RepartitionOp[_] => op }
      val coalesceDep = dependentStages
        .flatMap(id => stageGraph.stages(id).shuffleOperation.toSeq)
        .collectFirst { case op: Plan.CoalesceOp[_] => op }
      val partitionByDep = dependentStages
        .flatMap(id => stageGraph.stages(id).shuffleOperation.toSeq)
        .collectFirst { case op: Plan.PartitionByOp[_, _] => op }

      val writeF: F[ShuffleId] =
        if (hasSortByDependent) {
          // Find the dependent sortBy stage and use its configuration to range-partition output
          val sortByDependentId = dependentStages
            .find(depStageId =>
              stageGraph
                .stages(depStageId)
                .shuffleOperation
                .exists(_.isInstanceOf[Plan.SortByOp[_, _]]),
            )
            .get

          handleSortByRangePartitionedOutput(stageInfo, results, stageGraph, sortByDependentId)
        } else
          repartitionDep
            .map(op => handleRepartitionOrCoalesceOutput(stageInfo, results, op.numPartitions))
            .orElse(
              coalesceDep
                .map(op => handleRepartitionOrCoalesceOutput(stageInfo, results, op.numPartitions)),
            )
            .orElse(
              partitionByDep
                .map(op => handleRepartitionOrCoalesceOutput(stageInfo, results, op.numPartitions)),
            )
            .getOrElse(handleShuffleOutput(stageInfo, results))

      writeF.flatMap { shuffleId =>
        Sync[F].delay { stageToShuffleId(stageInfo.id) = shuffleId }
      }
    }
  }

  /**
   * Executes a narrow transformation stage. Now generic for input (T) and output (U) types.
   */
  private def executeNarrowStage[A, B](
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[A]],
      @annotation.unused stageToShuffleId: mutable.Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    val stage = stageInfo.stage.asInstanceOf[Stage[A, B]]
    val tasks = inputPartitions.map { partition => Task.StageTask(partition, stage) }
    scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
  }

  /**
   * Executes a shuffle stage by applying the appropriate shuffle operation. This version uses
   * generics to provide type safety for keys (K) and values (V).
   */
  private def executeShuffleStage[K, V](
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[(K, V)]],
      stageToShuffleId: mutable.Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    // Join is the only shuffle op that currently requires effectful scheduling work here
    stageInfo.shuffleOperation match {
      case Some(joinOp: Plan.JoinOp[_, _, _]) =>
        val tagged = stageInfo.inputSources.collect { case t: StageBuilder.TaggedShuffleFrom => t }
        val leftTagged = tagged
          .find(_.side == StageBuilder.Side.Left)
          .getOrElse(
            throw new IllegalStateException(
              s"Join missing Left input for stage ${stageInfo.id.toInt}",
            ),
          )
        val rightTagged = tagged
          .find(_.side == StageBuilder.Side.Right)
          .getOrElse(
            throw new IllegalStateException(
              s"Join missing Right input for stage ${stageInfo.id.toInt}",
            ),
          )
        val leftShuffleId = stageToShuffleId.getOrElse(
          leftTagged.stageId,
          throw new IllegalStateException(
            s"Missing shuffle id for Left upstream stage ${leftTagged.stageId.toInt}",
          ),
        )
        val rightShuffleId = stageToShuffleId.getOrElse(
          rightTagged.stageId,
          throw new IllegalStateException(
            s"Missing shuffle id for Right upstream stage ${rightTagged.stageId.toInt}",
          ),
        )
        val numPartitions = leftTagged.numPartitions // assume both sides same for now

        // Determine join strategy: use hint if available, otherwise auto-select
        val strategy =
          joinOp.joinStrategy.getOrElse(selectJoinStrategy(leftShuffleId, rightShuffleId))

        strategy match {
          case Plan.JoinStrategy.Broadcast =>
            executeBroadcastHashJoin(leftShuffleId, rightShuffleId, numPartitions)
          case Plan.JoinStrategy.SortMerge =>
            executeSortMergeJoin(leftShuffleId, rightShuffleId, numPartitions)
          case Plan.JoinStrategy.ShuffleHash =>
            executeShuffleHashJoin(leftShuffleId, rightShuffleId, numPartitions)
        }
      case _ =>
        Sync[F].delay(
          logger.debug(
            s"Executing shuffle stage ${stageInfo.id.toInt} with ${inputPartitions.size} input partitions",
          ),
        ) *>
          Sync[F].delay {
            val resultPartitions: Seq[Partition[_]] = stageInfo.shuffleOperation match {

              // Pattern match to capture the element type `a` and sorting key type `s`.
              case Some(sortBy: Plan.SortByOp[a, s]) =>
                // Input for sort is now key-value pairs of (sortKey, element)
                val typedPartitions = inputPartitions.asInstanceOf[Seq[Partition[(s, a)]]]
                implicit val ord: Ordering[s] = sortBy.ordering

                // Sort within each partition by key
                val sortedIters: Seq[Iterator[(s, a)]] =
                  typedPartitions.map(p => p.data.toSeq.sortBy(_._1)(ord).iterator)

                // K-way merge across partitions to ensure global order
                import scala.collection.mutable
                case class Head(idx: Int, pair: (s, a))
                implicit val heapOrd: Ordering[Head] = Ordering.by(_.pair._1)
                val heap = mutable.PriorityQueue.empty[Head](heapOrd.reverse)
                sortedIters.zipWithIndex.foreach { case (it, i) =>
                  if (it.hasNext) heap.enqueue(Head(i, it.next()))
                }
                val buffers = sortedIters.toArray

                val merged = mutable.ArrayBuffer[a]()
                while (heap.nonEmpty) {
                  val h = heap.dequeue()
                  merged += h.pair._2
                  val i = h.idx
                  if (buffers(i).hasNext) heap.enqueue(Head(i, buffers(i).next()))
                }

                Seq(Partition(merged.toSeq))

              // The remaining shuffle operations work on standard (K, V) pairs.
              case Some(Plan.GroupByKeyOp(_)) =>
                val allData = inputPartitions.flatMap(_.data)
                val groupedData = allData.groupBy(_._1).map { case (key, pairs) =>
                  (key, pairs.map(_._2))
                }
                Seq(Partition(groupedData.toSeq)).asInstanceOf[Seq[Partition[Any]]]

              case Some(reduceByKey: Plan.ReduceByKeyOp[_, _]) =>
                val allData = inputPartitions.flatMap(_.data)
                val reduceFunc = reduceByKey.reduceFunc.asInstanceOf[(V, V) => V]
                val reducedData = allData.groupBy(_._1).map { case (key, pairs) =>
                  val reducedValue = pairs
                    .map(_._2)
                    .reduceOption(reduceFunc)
                    .getOrElse(throw new NoSuchElementException(s"No values found for key $key"))
                  (key, reducedValue)
                }
                Seq(Partition(reducedData.toSeq))

              case Some(_: Plan.CoGroupOp[_, _, _]) =>
                val tagged = stageInfo.inputSources.collect {
                  case t: StageBuilder.TaggedShuffleFrom =>
                    t
                }
                val leftTagged = tagged
                  .find(_.side == StageBuilder.Side.Left)
                  .getOrElse(
                    throw new IllegalStateException(
                      s"Cogroup missing Left input for stage ${stageInfo.id.toInt}",
                    ),
                  )
                val rightTagged = tagged
                  .find(_.side == StageBuilder.Side.Right)
                  .getOrElse(
                    throw new IllegalStateException(
                      s"Cogroup missing Right input for stage ${stageInfo.id.toInt}",
                    ),
                  )
                val leftShuffleId = stageToShuffleId.getOrElse(
                  leftTagged.stageId,
                  throw new IllegalStateException(
                    s"Missing shuffle id for Left upstream stage ${leftTagged.stageId.toInt}",
                  ),
                )
                val rightShuffleId = stageToShuffleId.getOrElse(
                  rightTagged.stageId,
                  throw new IllegalStateException(
                    s"Missing shuffle id for Right upstream stage ${rightTagged.stageId.toInt}",
                  ),
                )
                val numPartitions = leftTagged.numPartitions
                val leftData = (0 until numPartitions).flatMap { partitionId =>
                  shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data
                }
                val rightData = (0 until numPartitions).flatMap { partitionId =>
                  shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data
                }

                // Group left and right data by key
                val leftByKey = leftData.groupBy(_._1)
                val rightByKey = rightData.groupBy(_._1)

                // Perform cogroup - include all keys from both sides
                val allKeys = leftByKey.keySet ++ rightByKey.keySet
                val cogroupedData = allKeys.map { key =>
                  val leftValues = leftByKey.getOrElse(key, Seq.empty).map(_._2)
                  val rightValues = rightByKey.getOrElse(key, Seq.empty).map(_._2)
                  (key, (leftValues, rightValues))
                }
                Seq(Partition(cogroupedData.toSeq))

              case Some(_: Plan.RepartitionOp[_]) | Some(_: Plan.CoalesceOp[_]) |
                  Some(_: Plan.PartitionByOp[_, _]) =>
                val typed = inputPartitions.asInstanceOf[Seq[Partition[(Any, Unit)]]]
                val out = typed.map { p => Partition(p.data.iterator.map(_._1).toList) }
                out

              // A default GroupByKey for any other unhandled shuffle operation.
              case _ =>
                val allData = inputPartitions.flatMap(_.data)
                val groupedData = allData.groupBy(_._1).map { case (key, pairs) =>
                  (key, pairs.map(_._2))
                }
                Seq(Partition(groupedData.toSeq))
            }
            resultPartitions
          }
    }
  }

  /**
   * Handles shuffle output. The cast is necessary as the results from `executeStage` are untyped.
   */
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  private def handleShuffleOutput(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
  ): F[ShuffleId] =
    Sync[F].delay {
      val keyValueResults = results.asInstanceOf[Seq[Partition[(Any, Any)]]]
      val shuffleData = shuffle.partitionByKey[Any, Any](
        data = keyValueResults,
        numPartitions = SparkletConf.get.defaultShufflePartitions,
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
  private def handleSortByRangePartitionedOutput(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
      stageGraph: StageBuilder.StageGraph,
      dependentSortByStageId: StageId,
  ): F[ShuffleId] =
    Sync[F].delay {
      stageGraph.stages(dependentSortByStageId).shuffleOperation match {
        case Some(sortBy: Plan.SortByOp[a, s]) =>
          given Ordering[s] = sortBy.ordering
          handleSortByRangePartitionedOutputTyped[a, s](
            stageInfo,
            results,
            stageGraph,
            dependentSortByStageId,
            sortBy,
          )
        case _ =>
          throw new IllegalStateException("Expected SortByOp for dependent stage but found none")
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def handleSortByRangePartitionedOutputTyped[A, S](
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
      stageGraph: StageBuilder.StageGraph,
      dependentSortByStageId: StageId,
      sortBy: Plan.SortByOp[A, S],
  )(using ordering: Ordering[S]): ShuffleId = {
    // Determine partition count for the dependent sort stage
    val expectedN = stageGraph
      .stages(dependentSortByStageId)
      .inputSources
      .collectFirst {
        case StageBuilder.ShuffleFrom(_, n) => n
        case StageBuilder.TaggedShuffleFrom(_, _, n) => n
      }
      .getOrElse(SparkletConf.get.defaultShufflePartitions)

    val elements: Seq[A] = results.flatMap(_.data.asInstanceOf[Iterable[A]])
    val keys: Seq[S] = elements.map(sortBy.keyFunc)

    val sample = takeKeySample(keys)
    val cutPoints = computeCutPoints(sample, expectedN)

    // Range partitioner using binary search over cut points
    val rangePartitioner = new Partitioner {
      def partition(key: Any, numPartitions: Int): Int = {
        if (numPartitions <= 1 || cutPoints.isEmpty) 0
        else {
          val k = key.asInstanceOf[S]
          // Find first index where k <= cutPoints(i), mapping to i, else last partition
          var lo = 0
          var hi = cutPoints.length - 1
          var ans = cutPoints.length
          while (lo <= hi) {
            val mid = (lo + hi) >>> 1
            val cmp = ordering.compare(k, cutPoints(mid))
            if (cmp <= 0) { ans = mid; hi = mid - 1 }
            else { lo = mid + 1 }
          }
          // ans in [0, P-1] maps to partition ans, tail goes to P-1
          math.min(ans, numPartitions - 1)
        }
      }
    }

    // Form (key, value) pairs per input partition
    val kvPartitions: Seq[Partition[(S, A)]] =
      results.asInstanceOf[Seq[Partition[A]]].map { p =>
        val it = p.data.iterator
        val buf = scala.collection.mutable.ArrayBuffer.empty[(S, A)]
        while (it.hasNext) {
          val a = it.next()
          buf += ((sortBy.keyFunc(a), a))
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
  private def takeKeySample[S](keys: Seq[S])(using ord: Ordering[S]): Seq[S] = {
    val perPart = SparkletConf.get.sortSamplePerPartition
    val maxSample = SparkletConf.get.sortMaxSample
    if (keys.isEmpty) Seq.empty
    else {
      val step = math.max(1, keys.size / math.max(1, SparkletConf.get.defaultShufflePartitions))
      val raw = keys.grouped(step).flatMap(_.take(perPart)).toSeq
      if (raw.size <= maxSample) raw else raw.take(maxSample)
    }
  }

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
        sorted(idx) // Now safe because sorted is a Vector and idx is clamped
      }.toVector
    }
  }

  /**
   * Handles shuffle output for repartition and coalesce operations by keying each element with a
   * Unit value and delegating to the shuffle service's partitioner.
   */
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  private def handleRepartitionOrCoalesceOutput(
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

  /**
   * Select the best join strategy based on data size heuristics.
   */
  private def selectJoinStrategy(
      leftShuffleId: ShuffleId,
      rightShuffleId: ShuffleId,
  ): Plan.JoinStrategy = {
    val leftSize = estimateShuffleSize(leftShuffleId)
    val rightSize = estimateShuffleSize(rightShuffleId)
    val broadcastThreshold = SparkletConf.get.broadcastJoinThreshold

    if (leftSize <= broadcastThreshold || rightSize <= broadcastThreshold) {
      Plan.JoinStrategy.Broadcast
    } else if (SparkletConf.get.enableSortMergeJoin) {
      Plan.JoinStrategy.SortMerge
    } else {
      Plan.JoinStrategy.ShuffleHash
    }
  }

  /**
   * Estimate the size of a shuffle dataset for join strategy selection.
   */
  private def estimateShuffleSize(shuffleId: ShuffleId): Long = {
    val numPartitions = shuffle.partitionCount(shuffleId)
    var totalSize = 0L
    for (partitionId <- 0 until numPartitions) {
      val partition = shuffle.readPartition[Any, Any](shuffleId, PartitionId(partitionId))
      totalSize += partition.data.size
    }
    totalSize
  }

  /**
   * Execute broadcast-hash join by broadcasting the smaller dataset.
   */
  private def executeBroadcastHashJoin(
      leftShuffleId: ShuffleId,
      rightShuffleId: ShuffleId,
      numPartitions: Int,
  ): F[Seq[Partition[_]]] = {
    val leftSize = estimateShuffleSize(leftShuffleId)
    val rightSize = estimateShuffleSize(rightShuffleId)

    if (leftSize <= rightSize) {
      // Broadcast left side, iterate over right side
      val leftData = (0 until numPartitions).flatMap { partitionId =>
        shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data
      }
      val broadcastId = SparkletRuntime.get.broadcast.broadcast(leftData)
      val broadcastData = SparkletRuntime.get.broadcast.getBroadcast[(Any, Any)](broadcastId)
      val broadcastMap = broadcastData.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
      val tasks = (0 until numPartitions).map { partitionId =>
        val rightLocalData: Seq[(Any, Any)] =
          shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data.toSeq
        Task.BroadcastHashJoinTask[Any, Any, Any](
          rightLocalData,
          broadcastMap,
          isRightLocal = true,
        )
      }
      scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
    } else {
      // Broadcast right side, iterate over left side
      val rightData = (0 until numPartitions).flatMap { partitionId =>
        shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data
      }
      val broadcastId = SparkletRuntime.get.broadcast.broadcast(rightData)
      val broadcastData = SparkletRuntime.get.broadcast.getBroadcast[(Any, Any)](broadcastId)
      val broadcastMap = broadcastData.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
      val tasks = (0 until numPartitions).map { partitionId =>
        val leftLocalData: Seq[(Any, Any)] =
          shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data.toSeq
        Task.BroadcastHashJoinTask[Any, Any, Any](
          leftLocalData,
          broadcastMap,
          isRightLocal = false,
        )
      }
      scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
    }
  }

  /**
   * Execute sort-merge join by sorting both sides before merging.
   */
  private def executeSortMergeJoin(
      leftShuffleId: ShuffleId,
      rightShuffleId: ShuffleId,
      numPartitions: Int,
  ): F[Seq[Partition[_]]] = {
    // For sort-merge join, we need to ensure data is sorted by key within each partition
    val tasks = (0 until numPartitions).map { partitionId =>
      val leftData: Seq[(Any, Any)] =
        shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data.toSeq
      val rightData: Seq[(Any, Any)] =
        shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data.toSeq

      // Use Any ordering for simplicity - in production would use proper key ordering
      given Ordering[Any] = Ordering.fromLessThan((a, b) => a.hashCode() < b.hashCode())
      Task.SortMergeJoinTask[Any, Any, Any](leftData, rightData)
    }
    scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
  }

  /**
   * Execute shuffle-hash join (the original implementation).
   */
  private def executeShuffleHashJoin(
      leftShuffleId: ShuffleId,
      rightShuffleId: ShuffleId,
      numPartitions: Int,
  ): F[Seq[Partition[_]]] = {
    val tasks = (0 until numPartitions).map { partitionId =>
      val l: Seq[(Any, Any)] =
        shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data.toSeq
      val r: Seq[(Any, Any)] =
        shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data.toSeq
      Task.ShuffleHashJoinTask[Any, Any, Any](l, r)
    }
    scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
  }

object DAGScheduler:
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  private def topologicalSort(dependencies: Map[StageId, Set[StageId]]): List[StageId] = {
    val inDegree = mutable.Map[StageId, Int]()
    val allStages = dependencies.keys.toSet ++ dependencies.values.flatten

    for (stage <- allStages) {
      inDegree(stage) = 0
    }

    for ((stage, deps) <- dependencies; _ <- deps) {
      inDegree(stage) = inDegree(stage) + 1
    }

    val queue = mutable.Queue[StageId]()
    for ((stage, degree) <- inDegree if degree == 0) {
      queue.enqueue(stage)
    }

    val result = mutable.ListBuffer[StageId]()

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      result += current
      for ((stage, deps) <- dependencies if deps.contains(current)) {
        inDegree(stage) = inDegree(stage) - 1
        if (inDegree(stage) == 0) queue.enqueue(stage)
      }
    }
    if (allStages.sizeIs != result.size)
      throw new IllegalStateException("Cycle detected in stage dependencies")
    result.toList
  }

  def requiresDAGScheduling[A](plan: Plan[A]): Boolean = {
    def containsShuffleOps(p: Plan[_]): Boolean = p match {
      case _: Plan.GroupByKeyOp[_, _] | _: Plan.ReduceByKeyOp[_, _] | _: Plan.SortByOp[_, _] |
          _: Plan.JoinOp[_, _, _] | _: Plan.CoGroupOp[_, _, _] | _: Plan.RepartitionOp[_] |
          _: Plan.CoalesceOp[_] | _: Plan.PartitionByOp[_, _] =>
        true
      case Plan.MapOp(source, _) => containsShuffleOps(source)
      case Plan.FilterOp(source, _) => containsShuffleOps(source)
      case Plan.FlatMapOp(source, _) => containsShuffleOps(source)
      case Plan.MapPartitionsOp(source, _) => containsShuffleOps(source)
      case Plan.DistinctOp(source) => containsShuffleOps(source)
      case Plan.KeysOp(source) => containsShuffleOps(source)
      case Plan.ValuesOp(source) => containsShuffleOps(source)
      case Plan.MapValuesOp(source, _) => containsShuffleOps(source)
      case Plan.FilterKeysOp(source, _) => containsShuffleOps(source)
      case Plan.FilterValuesOp(source, _) => containsShuffleOps(source)
      case Plan.FlatMapValuesOp(source, _) => containsShuffleOps(source)
      case Plan.UnionOp(left, right) => containsShuffleOps(left) || containsShuffleOps(right)
      case _ => false
    }
    containsShuffleOps(plan)
  }
