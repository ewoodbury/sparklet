package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.{Plan, Partition}
import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
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
      val results = executeStage(stageInfo, inputPartitions)
      
      stageResults(stageId) = results
      
      val dependentStages = stageGraph.dependencies.filter(_._2.contains(stageId)).keys
      val needsShuffleOutput = dependentStages.exists(depStageId => 
        stageGraph.stages(depStageId).isShuffleStage
      )
      
      if (needsShuffleOutput) {
        val actualShuffleId = handleShuffleOutput(stageInfo, results)
        stageToShuffleId(stageId) = actualShuffleId
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
    if (allStages.sizeIs != result.size) throw new IllegalStateException("Cycle detected in stage dependencies")
    result.toList
  }

  /**
   * Gets input partitions for a stage, returning them with a wildcard type `_`.
   * This avoids casting at the source, deferring it to the execution function.
   */
  private def getInputPartitionsForStage(
    stageInfo: StageBuilder.StageInfo, 
    @annotation.unused stageResults: mutable.Map[Int, Seq[Partition[_]]],
    stageToShuffleId: mutable.Map[Int, Int]
  ): Seq[Partition[_]] = {
    stageInfo.inputSources.flatMap {
      case StageBuilder.SourceInput(partitions) => 
        partitions // No cast needed
        
      case StageBuilder.ShuffleInput(plannedShuffleId, numPartitions) =>
        val sourceStages = stageToShuffleId.keys.filter(_ < stageInfo.id)
        val actualShuffleId = sourceStages.headOption.flatMap(stageToShuffleId.get).getOrElse(plannedShuffleId)
        
        // Shuffle data is always key-value, read as (Any, Any) since types are erased.
        (0 until numPartitions).map { partitionId =>
          ShuffleManager.readShufflePartition[Any, Any](actualShuffleId, partitionId)
        }
    }
  }

  /**
   * Executes a single stage, dispatching to a narrow or shuffle implementation.
   * This function now handles the necessary casting based on the stage type.
   */
  private def executeStage(
    stageInfo: StageBuilder.StageInfo, 
    inputPartitions: Seq[Partition[_]]
  ): Seq[Partition[_]] = {
    if (inputPartitions.isEmpty) {
      println(s"Warning: Stage ${stageInfo.id} has no input partitions")
      return Seq.empty
    }
    
    stageInfo match {
      case info if info.isShuffleStage =>
        // Cast to key-value pairs for shuffle stages.
        val kvPartitions = inputPartitions.asInstanceOf[Seq[Partition[(Any, Any)]]]
        executeShuffleStage[Any, Any](info, kvPartitions)

      case info =>
        // Cast to a generic partition for narrow stages.
        val anyPartitions = inputPartitions.asInstanceOf[Seq[Partition[Any]]]
        executeNarrowStage[Any, Any](info, anyPartitions)
    }
  }

  /**
   * Executes a narrow transformation stage. Now generic for input (T) and output (U) types.
   */
     private def executeNarrowStage[A, B](
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[A]]
   ): Seq[Partition[B]] = {
      // Cast the abstract stage to its specific A -> B transformation.
      val stage = stageInfo.stage.asInstanceOf[Stage[A, B]]
    
      val tasks = inputPartitions.map { partition =>
        Task.StageTask(partition, stage)
      }
    
      TaskScheduler.submit(tasks)
  }

  /**
   * Executes a shuffle stage by applying the appropriate shuffle operation.
   * This version uses generics to provide type safety for keys (K) and values (V).
   */
  private def executeShuffleStage[K, V](
    stageInfo: StageBuilder.StageInfo,
    inputPartitions: Seq[Partition[(K, V)]]
  ): Seq[Partition[Any]] = {
    println(s"Executing shuffle stage ${stageInfo.id} with ${inputPartitions.size} input partitions")
    val allData: Seq[(K, V)] = inputPartitions.flatMap(_.data)

    val resultPartitions: Seq[Partition[Any]] = stageInfo.shuffleOperation match {
      case Some(Plan.GroupByKeyOp(_)) =>
        val groupedData = allData.groupBy(_._1).map { 
            case (key, pairs) => (key, pairs.map(_._2)) 
        }
        Seq(Partition[Any](groupedData.toSeq))

      case Some(reduceByKey: Plan.ReduceByKeyOp[_, _]) =>
        val reduceFunc = reduceByKey.reduceFunc.asInstanceOf[(V, V) => V]
        val reducedData = allData.groupBy(_._1).map { case (key, pairs) =>
          val reducedValue = pairs.map(_._2).reduceOption(reduceFunc)
            .getOrElse(throw new NoSuchElementException(s"No values found for key $key"))
          (key, reducedValue)
        }
        Seq(Partition[Any](reducedData.toSeq))

      case Some(sortBy: Plan.SortByOp[_, _]) =>
        Seq(Partition[Any](allData))

      case _ =>
        val groupedData = allData.groupBy(_._1).map { case (key, pairs) => (key, pairs.map(_._2)) }
        Seq(Partition[Any](groupedData.toSeq))
    }
    resultPartitions
  }

  /**
   * Handles shuffle output. The cast is necessary as the results from `executeStage` are untyped.
   */
  private def handleShuffleOutput(
    stageInfo: StageBuilder.StageInfo,
    results: Seq[Partition[_]]
  ): Int = {
    // Shuffle data is always key-value data.
    val keyValueResults = results.asInstanceOf[Seq[Partition[(Any, Any)]]]
    val shuffleData = ShuffleManager.partitionByKey[Any, Any](keyValueResults, results.size)
    val actualShuffleId = ShuffleManager.writeShuffleData[Any, Any](shuffleData)
    
    println(s"Stored shuffle data for stage ${stageInfo.id} with shuffle ID $actualShuffleId")
    actualShuffleId
  }

  def requiresDAGScheduling[A](plan: Plan[A]): Boolean = {
    def containsShuffleOps(p: Plan[_]): Boolean = p match {
      case _: Plan.GroupByKeyOp[_, _] | _: Plan.ReduceByKeyOp[_, _] | 
           _: Plan.SortByOp[_, _] | _: Plan.JoinOp[_, _, _] | _: Plan.CoGroupOp[_, _, _] => true
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