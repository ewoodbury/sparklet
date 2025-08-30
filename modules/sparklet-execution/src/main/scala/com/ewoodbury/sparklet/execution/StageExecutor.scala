package com.ewoodbury.sparklet.execution

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.{Partition, PartitionId, Plan, ShuffleId, StageId}
import com.ewoodbury.sparklet.runtime.api.{ShuffleService, TaskScheduler}

/**
 * Executor for handling stage execution.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.MutableDataStructures",
    "org.wartremover.warts.Equals",
  ),
)
final class StageExecutor[F[_]: Sync](
    shuffle: ShuffleService,
    scheduler: TaskScheduler[F],
    joinExecutor: JoinExecutor[F],
) extends StrictLogging:

  /**
   * Gets input partitions for a stage, returning them with a wildcard type `_`. This avoids
   * casting at the source, deferring it to the execution function.
   */
  def getInputPartitionsForStage(
      stageInfo: StageBuilder.StageInfo,
      stageResults: Map[StageId, Seq[Partition[_]]],
      stageToShuffleId: Map[StageId, ShuffleId],
  ): Seq[Partition[_]] = {
    stageInfo.inputSources.flatMap {
      case StageBuilder.SourceInput(partitions) =>
        partitions // No cast needed

      case StageBuilder.StageOutput(depStageId) =>
        // Read the already computed output of the dependent stage
        stageResults.getOrElse(depStageId, Seq.empty[Partition[_]])

      case StageBuilder.ShuffleInput(upstreamStageId, _side, numPartitions) =>
        // Handle both single-input and multi-input shuffle operations uniformly
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
   * Executes a single stage using type-safe dispatch based on stage operations.
   * Uses typed helpers to eliminate unsafe casting and improve.
   */
  def executeStage(
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[_]],
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    if (inputPartitions.isEmpty) then
      Sync[F].delay(logger.warn(s"Stage ${stageInfo.id.toInt} has no input partitions")) *> Sync[F]
        .pure(Seq.empty)
    else {
      stageInfo match {
        case info if info.isShuffleStage =>
          executeShuffleStageTyped(info, inputPartitions, stageToShuffleId)
        case info =>
          executeNarrowStageTyped(info, inputPartitions, stageToShuffleId)
      }
    }
  }

  /**
   * Type-safe execution of narrow stages using the Operation ADT.
   * 
   * Wraps around the generic executeNarrowStage method, casting inputs to `Any`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def executeNarrowStageTyped(
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[_]],
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    // For now, cast to Any types and execute. Plan to use Operation ADT in future for better safety.
    val anyPartitions = inputPartitions.asInstanceOf[Seq[Partition[Any]]]
    executeNarrowStage(stageInfo, anyPartitions, stageToShuffleId)
  }

  /**
   * Type-safe execution of shuffle stages using operation-specific handlers.
   */
  private def executeShuffleStageTyped(
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[_]],
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    stageInfo.shuffleOperation match {
      case Some(joinOp: Plan.JoinOp[_, _, _]) =>
        executeJoinOperation(stageInfo, stageToShuffleId)
      case Some(sortBy: Plan.SortByOp[_, _]) =>
        executeSortByOperation(sortBy, inputPartitions)
      case Some(groupByKey: Plan.GroupByKeyOp[_, _]) =>
        executeGroupByKeyOperation(inputPartitions)
      case Some(reduceByKey: Plan.ReduceByKeyOp[_, _]) =>
        executeReduceByKeyOperation(reduceByKey, inputPartitions)
      case Some(cogroup: Plan.CoGroupOp[_, _, _]) =>
        executeCoGroupOperation(stageInfo, stageToShuffleId)
      case Some(_: Plan.RepartitionOp[_]) | Some(_: Plan.CoalesceOp[_]) | Some(_: Plan.PartitionByOp[_, _]) =>
        executeRepartitionOperation(inputPartitions)
      case _ =>
        // Default to GroupByKey behavior for unknown operations
        executeGroupByKeyOperation(inputPartitions)
    }
  }

  /**
   * Type-safe handler for join operations.
   */
  private def executeJoinOperation(
      stageInfo: StageBuilder.StageInfo,
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    stageInfo.shuffleOperation match {
      case Some(joinOp: Plan.JoinOp[_, _, _]) =>
        val shuffleInputs = stageInfo.inputSources.collect { case s: StageBuilder.ShuffleInput => s }
        val leftInput = shuffleInputs
          .find(_.side.contains(StageBuilder.Side.Left))
          .getOrElse(
            throw new IllegalStateException(
              s"Join missing Left input for stage ${stageInfo.id.toInt}",
            ),
          )
        val rightInput = shuffleInputs
          .find(_.side.contains(StageBuilder.Side.Right))
          .getOrElse(
            throw new IllegalStateException(
              s"Join missing Right input for stage ${stageInfo.id.toInt}",
            ),
          )
        val leftShuffleId = stageToShuffleId.getOrElse(
          leftInput.stageId,
          throw new IllegalStateException(
            s"Missing shuffle id for Left upstream stage ${leftInput.stageId.toInt}",
          ),
        )
        val rightShuffleId = stageToShuffleId.getOrElse(
          rightInput.stageId,
          throw new IllegalStateException(
            s"Missing shuffle id for Right upstream stage ${rightInput.stageId.toInt}",
          ),
        )
        val numPartitions = leftInput.numPartitions

        // Determine join strategy
        val strategy =
          joinOp.joinStrategy.getOrElse(
            joinExecutor.selectJoinStrategy(leftShuffleId, rightShuffleId),
          )

        logger.info(s"Using join strategy: $strategy for stage ${stageInfo.id.toInt}")

        strategy match {
          case Plan.JoinStrategy.Broadcast =>
            joinExecutor.executeBroadcastHashJoin(leftShuffleId, rightShuffleId, numPartitions)
          case Plan.JoinStrategy.SortMerge =>
            joinExecutor.executeSortMergeJoin(leftShuffleId, rightShuffleId, numPartitions)
          case Plan.JoinStrategy.ShuffleHash =>
            joinExecutor.executeShuffleHashJoin(leftShuffleId, rightShuffleId, numPartitions)
        }
      case _ =>
        Sync[F].raiseError(new IllegalStateException("Join operation expected but not found"))
    }
  }

  /**
   * Type-safe handler for SortBy operations.
   */
  private def executeSortByOperation[A, S](
      sortBy: Plan.SortByOp[A, S],
      inputPartitions: Seq[Partition[_]],
  ): F[Seq[Partition[_]]] = {
    Sync[F].delay {
      // Input for sort is key-value pairs of (sortKey, element)
      val typedPartitions = inputPartitions.asInstanceOf[Seq[Partition[(S, A)]]]
      implicit val ord: Ordering[S] = sortBy.ordering

      // Sort within each partition by key
      val sortedIters: Seq[Iterator[(S, A)]] =
        typedPartitions.map(p => p.data.toSeq.sortBy(_._1)(ord).iterator)

      // K-way merge across partitions to ensure global order
      import scala.collection.mutable
      case class Head(idx: Int, pair: (S, A))
      implicit val heapOrd: Ordering[Head] = Ordering.by(_.pair._1)
      val heap = mutable.PriorityQueue.empty[Head](heapOrd.reverse)
      sortedIters.zipWithIndex.foreach { case (it, i) =>
        if (it.hasNext) heap.enqueue(Head(i, it.next()))
      }
      val buffers = sortedIters.toArray

      val merged = mutable.ArrayBuffer[A]()
      while (heap.nonEmpty) {
        val h = heap.dequeue()
        merged += h.pair._2
        val i = h.idx
        if (buffers(i).hasNext) heap.enqueue(Head(i, buffers(i).next()))
      }

      Seq(Partition(merged.toSeq))
    }
  }

  /**
   * Type-safe handler for GroupByKey operations.
   */
  private def executeGroupByKeyOperation(
      inputPartitions: Seq[Partition[_]],
  ): F[Seq[Partition[_]]] = {
    Sync[F].delay {
      val allData = inputPartitions.flatMap(_.data).asInstanceOf[Seq[(Any, Any)]]
      val groupedData = allData.groupBy(_._1).map { case (key, pairs) =>
        (key, pairs.map(_._2))
      }
      Seq(Partition(groupedData.toSeq)).asInstanceOf[Seq[Partition[Any]]]
    }
  }

  /**
   * Type-safe handler for ReduceByKey operations.
   */
  private def executeReduceByKeyOperation[K, V](
      reduceByKey: Plan.ReduceByKeyOp[K, V],
      inputPartitions: Seq[Partition[_]],
  ): F[Seq[Partition[_]]] = {
    Sync[F].delay {
      val allData = inputPartitions.flatMap(_.data)
      val reduceFunc = reduceByKey.reduceFunc
      val reducedData = allData.asInstanceOf[Seq[(K, V)]].groupBy(_._1).map { case (key, pairs) =>
        val reducedValue = pairs
          .map(_._2)
          .reduceOption(reduceFunc)
          .getOrElse(throw new NoSuchElementException(s"No values found for key $key"))
        (key, reducedValue)
      }
      Seq(Partition(reducedData.toSeq))
    }
  }

  /**
   * Type-safe handler for CoGroup operations.
   */
  private def executeCoGroupOperation(
      stageInfo: StageBuilder.StageInfo,
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    Sync[F].delay {
      val shuffleInputs = stageInfo.inputSources.collect {
        case s: StageBuilder.ShuffleInput => s
      }
      val leftInput = shuffleInputs
        .find(_.side.contains(StageBuilder.Side.Left))
        .getOrElse(
          throw new IllegalStateException(
            s"Cogroup missing Left input for stage ${stageInfo.id.toInt}",
          ),
        )
      val rightInput = shuffleInputs
        .find(_.side.contains(StageBuilder.Side.Right))
        .getOrElse(
          throw new IllegalStateException(
            s"Cogroup missing Right input for stage ${stageInfo.id.toInt}",
          ),
        )
      val leftShuffleId = stageToShuffleId.getOrElse(
        leftInput.stageId,
        throw new IllegalStateException(
          s"Missing shuffle id for Left upstream stage ${leftInput.stageId.toInt}",
        ),
      )
      val rightShuffleId = stageToShuffleId.getOrElse(
        rightInput.stageId,
        throw new IllegalStateException(
          s"Missing shuffle id for Right upstream stage ${rightInput.stageId.toInt}",
        ),
      )
      val numPartitions = leftInput.numPartitions
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
    }
  }

  /**
   * Type-safe handler for repartition operations.
   */
  private def executeRepartitionOperation(
      inputPartitions: Seq[Partition[_]],
  ): F[Seq[Partition[_]]] = {
    Sync[F].delay {
      val typed = inputPartitions.asInstanceOf[Seq[Partition[(Any, Unit)]]]
      val out = typed.map { p => Partition(p.data.iterator.map(_._1).toList) }
      out
    }
  }

  /**
   * Executes a narrow transformation stage. Now generic for input (T) and output (U) types.
   */
  private def executeNarrowStage[A, B](
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[A]],
      @annotation.unused stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    val stage = stageInfo.stage.asInstanceOf[Stage[A, B]]
    val tasks = inputPartitions.map { partition => Task.StageTask(partition, stage) }
    scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
  }


