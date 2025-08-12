package com.ewoodbury.sparklet.execution

import cats.effect.kernel.Sync
import cats.syntax.all.*

import scala.collection.mutable

import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.*
import com.ewoodbury.sparklet.runtime.api.{Partitioner, ShuffleService, TaskScheduler}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.MutableDataStructures",
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Equals",
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
      Sync[F].delay(logger.warn(s"Stage ${stageInfo.id.toInt} has no input partitions")) *> Sync[F].pure(Seq.empty)
    else
      {
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

    val needsShuffleOutput = dependentStages.exists(depStageId =>
      stageGraph.stages(depStageId).isShuffleStage,
    )

    if (!needsShuffleOutput) Sync[F].unit
    else {
      val hasSortByDependent = dependentStages.exists(depStageId =>
        stageGraph
          .stages(depStageId)
          .shuffleOperation
          .exists(_.isInstanceOf[Plan.SortByOp[_, _]]),
      )

      val writeF: F[ShuffleId] =
        if (hasSortByDependent) handleSortByShuffleOutput(stageInfo, results)
        else handleShuffleOutput(stageInfo, results)

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
  ): F[Seq[Partition[_]]] =
    {
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
  ): F[Seq[Partition[_]]] =
    Sync[F].delay(logger.debug(
      s"Executing shuffle stage ${stageInfo.id.toInt} with ${inputPartitions.size} input partitions",
    )) *>
      Sync[F].delay {
        val resultPartitions: Seq[Partition[_]] = stageInfo.shuffleOperation match {

      // Pattern match to capture the element type `a` and sorting key type `s`.
      case Some(sortBy: Plan.SortByOp[a, s]) =>
        // Assert that the input for this operation has the shape (a, Unit),
        // where 'a' is the data element that was packed into a tuple for the shuffle.
        val typedPartitions = inputPartitions.asInstanceOf[Seq[Partition[(a, Unit)]]]
        val inputData: Seq[a] = typedPartitions.flatMap(_.data.map(_._1))

        // The `keyFunc` and `ordering` are now correctly typed, inherited from the Plan.
        implicit val ord: Ordering[s] = sortBy.ordering
        val sortedData = inputData.sortBy(sortBy.keyFunc)

        Seq(Partition(sortedData))

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

      case Some(_: Plan.JoinOp[_, _, _]) =>
        // Resolve explicit left/right tagged inputs and read exactly those shuffles
        val tagged = stageInfo.inputSources.collect { case t: StageBuilder.TaggedShuffleFrom => t }
        val leftTagged = tagged.find(_.side == StageBuilder.Side.Left).getOrElse(
          throw new IllegalStateException(s"Join missing Left input for stage ${stageInfo.id.toInt}")
        )
        val rightTagged = tagged.find(_.side == StageBuilder.Side.Right).getOrElse(
          throw new IllegalStateException(s"Join missing Right input for stage ${stageInfo.id.toInt}")
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
        val leftData = (0 until numPartitions).flatMap { partitionId =>
          shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data
        }
        val rightData = (0 until numPartitions).flatMap { partitionId =>
          shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data
        }

        // Group left and right data by key
        val leftByKey = leftData.groupBy(_._1)
        val rightByKey = rightData.groupBy(_._1)

        // Perform inner join with cartesian product for matching keys
        val joinedData = for {
          (key, leftValues) <- leftByKey.toSeq
          rightValues <- rightByKey.get(key).toSeq
          leftValue <- leftValues.map(_._2)
          rightValue <- rightValues.map(_._2)
        } yield (key, (leftValue, rightValue))
        Seq(Partition(joinedData))

      case Some(_: Plan.CoGroupOp[_, _, _]) =>
        val tagged = stageInfo.inputSources.collect { case t: StageBuilder.TaggedShuffleFrom => t }
        val leftTagged = tagged.find(_.side == StageBuilder.Side.Left).getOrElse(
          throw new IllegalStateException(s"Cogroup missing Left input for stage ${stageInfo.id.toInt}")
        )
        val rightTagged = tagged.find(_.side == StageBuilder.Side.Right).getOrElse(
          throw new IllegalStateException(s"Cogroup missing Right input for stage ${stageInfo.id.toInt}")
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
  private def handleSortByShuffleOutput(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
  ): F[ShuffleId] =
    Sync[F].delay {
      val allData: Seq[Any] = results.flatMap(_.data)
      val keyedData: Seq[(Any, Unit)] = allData.map(element => (element, ()))
      val shuffleData = ShuffleService.ShuffleData(Map(PartitionId(0) -> keyedData))
      val actualShuffleId = shuffle.write(shuffleData)
      logger.debug(
        s"Stored sortBy shuffle data for stage ${stageInfo.id.toInt} with shuffle ID ${actualShuffleId.toInt}",
      )
      actualShuffleId
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
          _: Plan.JoinOp[_, _, _] | _: Plan.CoGroupOp[_, _, _] =>
        true
      case Plan.MapOp(source, _) => containsShuffleOps(source)
      case Plan.FilterOp(source, _) => containsShuffleOps(source)
      case Plan.FlatMapOp(source, _) => containsShuffleOps(source)
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
