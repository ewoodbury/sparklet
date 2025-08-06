package com.ewoodbury.sparklet.execution

import scala.collection.mutable

import com.ewoodbury.sparklet.core.{Partition, Plan}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.MutableDataStructures",
    "org.wartremover.warts.Any",
  ),
)
object DAGScheduler:

  /**
   * Executes a plan using multi-stage execution, handling shuffle boundaries.
   */
  def execute[A](plan: Plan[A]): Iterable[A] = {
    println("--- DAGScheduler: Starting multi-stage execution ---")

    val stageGraph = StageBuilder.buildStageGraph(plan)
    println(s"DAGScheduler: Built stage graph with ${stageGraph.stages.size} stages")

    val executionOrder = topologicalSort(stageGraph.dependencies)
    println(s"DAGScheduler: Execution order: ${executionOrder.mkString(" -> ")}")

    val stageResults = mutable.Map[Int, Seq[Partition[_]]]()
    val stageToShuffleId = mutable.Map[Int, Int]()

    for (stageId <- executionOrder) {
      println(s"DAGScheduler: Executing stage $stageId")
      val stageInfo = stageGraph.stages(stageId)
      val inputPartitions = getInputPartitionsForStage(stageInfo, stageResults, stageToShuffleId)
      val results = executeStage(stageInfo, inputPartitions, stageToShuffleId)
      
      stageResults(stageId) = results

      val dependentStages = stageGraph.dependencies.filter(_._2.contains(stageId)).keys
      val needsShuffleOutput =
        dependentStages.exists(depStageId => stageGraph.stages(depStageId).isShuffleStage) ||
        (stageInfo.isShuffleStage && dependentStages.nonEmpty)

      if (needsShuffleOutput) {
        // Check if the dependent stage is a sortBy operation
        val dependentSortByStage = dependentStages.find(depStageId =>
          stageGraph
            .stages(depStageId)
            .shuffleOperation
            .exists(_.isInstanceOf[Plan.SortByOp[_, _]]),
        )

        if (dependentSortByStage.isDefined) {
          // For sortBy, we don't need to partition the data, just store it
          val actualShuffleId = handleSortByShuffleOutput(stageInfo, results)
          stageToShuffleId(stageId) = actualShuffleId
        } else {
          // Regular key-value shuffle operations
          val actualShuffleId = handleShuffleOutput(stageInfo, results)
          stageToShuffleId(stageId) = actualShuffleId
        }
      }
    }

    val finalResults = stageResults(stageGraph.finalStageId)
    val finalData = finalResults.flatMap(_.data.asInstanceOf[Iterable[A]])

    println("DAGScheduler: Multi-stage execution completed")
    finalData
  }

  private def topologicalSort(dependencies: Map[Int, Set[Int]]): List[Int] = {
    val inDegree = mutable.Map[Int, Int]()
    val allStages = dependencies.keys.toSet ++ dependencies.values.flatten

    for (stage <- allStages) {
      inDegree(stage) = 0
    }

    for ((stage, deps) <- dependencies; dep <- deps) {
      inDegree(stage) = inDegree(stage) + 1
    }

    val queue = mutable.Queue[Int]()
    for ((stage, degree) <- inDegree if degree == 0) {
      queue.enqueue(stage)
    }

    val result = mutable.ListBuffer[Int]()

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

  /**
   * Gets input partitions for a stage, returning them with a wildcard type `_`. This avoids
   * casting at the source, deferring it to the execution function.
   */
  private def getInputPartitionsForStage(
      stageInfo: StageBuilder.StageInfo,
      @annotation.unused stageResults: mutable.Map[Int, Seq[Partition[_]]],
      stageToShuffleId: mutable.Map[Int, Int],
  ): Seq[Partition[_]] = {
    stageInfo.inputSources.flatMap {
      case StageBuilder.SourceInput(partitions) =>
        partitions // No cast needed

      case StageBuilder.ShuffleInput(plannedShuffleId, numPartitions) =>
        // For join operations, we need to handle multiple shuffle inputs differently
        stageInfo.shuffleOperation match {
          case Some(_: Plan.JoinOp[_, _, _]) =>
            // For joins, read from all available shuffle IDs (left and right)
            val dependentStages = stageToShuffleId.keys.filter(_ < stageInfo.id).toSeq.sorted
            dependentStages.flatMap { stageId =>
              val shuffleId = stageToShuffleId(stageId)
              (0 until numPartitions).map { partitionId =>
                ShuffleManager.readShufflePartition[Any, Any](shuffleId, partitionId)
              }
            }

          // For Cogroup, we also need to handle multiple shuffle inputs
          case Some(_: Plan.CoGroupOp[_, _, _]) =>
            // For cogroups, read from all available shuffle IDs (left and right)
            val dependentStages = stageToShuffleId.keys.filter(_ < stageInfo.id).toSeq.sorted
            dependentStages.flatMap { stageId =>
              val shuffleId = stageToShuffleId(stageId)
              (0 until numPartitions).map { partitionId =>
                ShuffleManager.readShufflePartition[Any, Any](shuffleId, partitionId)
              }
            }

          case _ =>
            // Regular shuffle operations - read from the most recent shuffle
            val sourceStages = stageToShuffleId.keys.filter(_ < stageInfo.id)
            val actualShuffleId =
              sourceStages.maxOption.flatMap(stageToShuffleId.get).getOrElse(plannedShuffleId)

            // Shuffle data is always key-value, read as (Any, Any) since types are erased.
            (0 until numPartitions).map { partitionId =>
              ShuffleManager.readShufflePartition[Any, Any](actualShuffleId, partitionId)
            }
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
      stageToShuffleId: mutable.Map[Int, Int],
  ): Seq[Partition[_]] = {
    if (inputPartitions.isEmpty) {
      println(s"Warning: Stage ${stageInfo.id} has no input partitions")
      return Seq.empty
    }

    stageInfo match {
      case info if info.isShuffleStage =>
        // Cast to key-value pairs for shuffle stages.
        // For sortBy, the data is not key-value pairs, but type is still cast to (Any, Any).
        val partitions = inputPartitions.asInstanceOf[Seq[Partition[(Any, Any)]]]
        executeShuffleStage(info, partitions, stageToShuffleId)

      case info =>
        // Cast to a generic partition for narrow stages.
        val anyPartitions = inputPartitions.asInstanceOf[Seq[Partition[Any]]]
        executeNarrowStage(info, anyPartitions, stageToShuffleId)
    }
  }

  /**
   * Executes a narrow transformation stage. Now generic for input (T) and output (U) types.
   */
  private def executeNarrowStage[A, B](
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[A]],
      @annotation.unused stageToShuffleId: mutable.Map[Int, Int],
  ): Seq[Partition[B]] = {
    // Cast the abstract stage to its specific A -> B transformation.
    val stage = stageInfo.stage.asInstanceOf[Stage[A, B]]

    val tasks = inputPartitions.map { partition =>
      Task.StageTask(partition, stage)
    }

    TaskScheduler.submit(tasks)
  }

  /**
   * Executes a shuffle stage by applying the appropriate shuffle operation. This version uses
   * generics to provide type safety for keys (K) and values (V).
   */
  private def executeShuffleStage[K, V](
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[(K, V)]],
      stageToShuffleId: mutable.Map[Int, Int],
  ): Seq[Partition[Any]] = {
    println(
      s"Executing shuffle stage ${stageInfo.id} with ${inputPartitions.size} input partitions",
    )

    val resultPartitions: Seq[Partition[Any]] = stageInfo.shuffleOperation match {

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

      case Some(joinOp: Plan.JoinOp[_, _, _]) =>
        // For join operations, we need to read left and right shuffle data separately
        // Get the dependent stages in order (left stage first, then right stage)
        val dependentStages = stageInfo.inputSources.collect {
          case StageBuilder.ShuffleInput(shuffleId, _) => shuffleId
        }
        
        if (dependentStages.lengthIs < 2) {
          throw new IllegalStateException(s"Join operation requires 2 shuffle inputs, got ${dependentStages.length}")
        }
        
        val leftShuffleId = stageToShuffleId.find(_._1 < stageInfo.id).map(_._2).getOrElse(dependentStages.headOption.getOrElse(0))
        val rightShuffleId = stageToShuffleId.filter(_._1 < stageInfo.id).toSeq.sortBy(_._1).drop(1).headOption.map(_._2).getOrElse(dependentStages.drop(1).headOption.getOrElse(1))
        
        // Read left and right data separately from their respective shuffle outputs
        val numPartitions = 4
        val leftData = (0 until numPartitions).flatMap { partitionId =>
          ShuffleManager.readShufflePartition[Any, Any](leftShuffleId, partitionId).data
        }
        val rightData = (0 until numPartitions).flatMap { partitionId =>
          ShuffleManager.readShufflePartition[Any, Any](rightShuffleId, partitionId).data
        }
        
        // Group left and right data by key
        val leftByKey = leftData.groupBy(_._1)
        val rightByKey = rightData.groupBy(_._1)
        
        // Perform inner join - only keep keys that appear in both sides
        val joinedData = leftByKey.flatMap { case (key, leftValues) =>
          rightByKey.get(key).map { rightValues =>
            // For simplicity, take the first value from each side
            // In a full implementation, this would be a cartesian product
            val leftValue = leftValues.headOption.map(_._2).getOrElse(throw new NoSuchElementException("No left value"))
            val rightValue = rightValues.headOption.map(_._2).getOrElse(throw new NoSuchElementException("No right value"))
            (key, (leftValue, rightValue))
          }
        }
        Seq(Partition(joinedData.toSeq))

      case Some(cogroupOp: Plan.CoGroupOp[_, _, _]) =>
        // For cogroup operations, we need to read left and right shuffle data separately
        // Get the dependent stages in order (left stage first, then right stage)
        val dependentStages = stageInfo.inputSources.collect {
          case StageBuilder.ShuffleInput(shuffleId, _) => shuffleId
        }
        
        if (dependentStages.lengthIs < 2) {
          throw new IllegalStateException(s"Cogroup operation requires 2 shuffle inputs, got ${dependentStages.length}")
        }
        
        val leftShuffleId = stageToShuffleId.find(_._1 < stageInfo.id).map(_._2).getOrElse(dependentStages.headOption.getOrElse(0))
        val rightShuffleId = stageToShuffleId.filter(_._1 < stageInfo.id).toSeq.sortBy(_._1).drop(1).headOption.map(_._2).getOrElse(dependentStages.drop(1).headOption.getOrElse(1))
        
        // Read left and right data separately from their respective shuffle outputs
        val numPartitions = 4
        val leftData = (0 until numPartitions).flatMap { partitionId =>
          ShuffleManager.readShufflePartition[Any, Any](leftShuffleId, partitionId).data
        }
        val rightData = (0 until numPartitions).flatMap { partitionId =>
          ShuffleManager.readShufflePartition[Any, Any](rightShuffleId, partitionId).data
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
  private def handleShuffleOutput(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
  ): Int = {
    // Shuffle data is always key-value data.
    val keyValueResults = results.asInstanceOf[Seq[Partition[(Any, Any)]]]
    val shuffleData = ShuffleManager.partitionByKey[Any, Any](keyValueResults, results.size)
    val actualShuffleId = ShuffleManager.writeShuffleData[Any, Any](shuffleData)

    println(s"Stored shuffle data for stage ${stageInfo.id} with shuffle ID $actualShuffleId")
    actualShuffleId
  }

  /**
   * Handles shuffle output for sortBy operations by mapping each data element of type T to a
   * key-value pair of type (T, Unit).
   */
  private def handleSortByShuffleOutput(
      stageInfo: StageBuilder.StageInfo,
      results: Seq[Partition[_]],
  ): Int = {
    val allData: Seq[Any] = results.flatMap(_.data)

    val keyedData: Seq[(Any, Unit)] = allData.map(element => (element, ()))

    val shuffleData = ShuffleManager.ShuffleData(Map(0 -> keyedData))

    val actualShuffleId = ShuffleManager.writeShuffleData(shuffleData)

    println(
      s"Stored sortBy shuffle data for stage ${stageInfo.id} with shuffle ID $actualShuffleId",
    )
    actualShuffleId
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
