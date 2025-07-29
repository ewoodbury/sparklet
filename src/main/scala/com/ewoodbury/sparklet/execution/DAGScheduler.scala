package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.{Plan, Partition}
import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))

/**
 * The DAG Scheduler orchestrates multi-stage execution of plans with shuffle operations.
 * It builds stage graphs, executes stages in dependency order, and manages shuffle data.
 */
object DAGScheduler:
  
  /**
   * Executes a plan using multi-stage execution, handling shuffle boundaries.
   * This is the main entry point for executing plans with wide transformations.
   */
  def execute[A](plan: Plan[A]): Iterable[A] = {
    println("--- DAGScheduler: Starting multi-stage execution ---")
    
    // Build the complete stage graph
    val stageGraph = StageBuilder.buildStageGraph(plan)
    println(s"DAGScheduler: Built stage graph with ${stageGraph.stages.size} stages")
    
    // Execute stages in topological order
    val executionOrder = topologicalSort(stageGraph.dependencies)
    println(s"DAGScheduler: Execution order: ${executionOrder.mkString(" -> ")}")
    
    // Storage for intermediate results from each stage
    val stageResults = mutable.Map[Int, Seq[Partition[_]]]()
    // Track actual shuffle IDs written by stages
    val stageToShuffleId = mutable.Map[Int, Int]()
    
    // Execute each stage in order
    for (stageId <- executionOrder) {
      println(s"DAGScheduler: Executing stage $stageId")
      val stageInfo = stageGraph.stages(stageId)
      
      // Get input partitions for this stage
      val inputPartitions = getInputPartitionsForStage(stageInfo, stageResults, stageToShuffleId)
      
      // Create and execute tasks for this stage
      val results = executeStage(stageInfo, inputPartitions)
      
      // Store results for dependent stages
      stageResults(stageId) = results
      
      // Check if any dependent stages need shuffle data from this stage
      val dependentStages = stageGraph.dependencies.filter(_._2.contains(stageId)).keys
      val needsShuffleOutput = dependentStages.exists(depStageId => 
        stageGraph.stages(depStageId).isShuffleStage
      )
      
      // If dependent stages need shuffle data, write it to ShuffleManager
      if (needsShuffleOutput) {
        val actualShuffleId = handleShuffleOutput(stageInfo, results)
        stageToShuffleId(stageId) = actualShuffleId
      }
    }
    
    // Return results from the final stage
    val finalResults = stageResults(stageGraph.finalStageId)
    val finalData = finalResults.flatMap(_.data.asInstanceOf[Iterable[A]])
    
    println("DAGScheduler: Multi-stage execution completed")
    finalData
  }
  
  /**
   * Performs topological sort on stage dependencies to determine execution order.
   */
  private def topologicalSort(dependencies: Map[Int, Set[Int]]): List[Int] = {
    val inDegree = mutable.Map[Int, Int]()
    val allStages = dependencies.keys.toSet ++ dependencies.values.flatten
    
    // Initialize in-degrees
    for (stage <- allStages) {
      inDegree(stage) = 0
    }
    
    // Calculate in-degrees
    for ((stage, deps) <- dependencies; dep <- deps) {
      inDegree(stage) = inDegree(stage) + 1
    }
    
    // Kahn's algorithm for topological sorting
    val queue = mutable.Queue[Int]()
    val result = mutable.ListBuffer[Int]()
    
    // Add stages with no dependencies
    for ((stage, degree) <- inDegree if degree == 0) {
      queue.enqueue(stage)
    }
    
    while (queue.nonEmpty) {
      val current = queue.dequeue()
      result += current
      
      // Reduce in-degree for dependent stages
      for ((stage, deps) <- dependencies if deps.contains(current)) {
        inDegree(stage) = inDegree(stage) - 1
        if (inDegree(stage) == 0) {
          queue.enqueue(stage)
        }
      }
    }
    
    if (allStages.sizeIs != result.size) {
      throw new IllegalStateException("Cycle detected in stage dependencies")
    }
    
    result.toList
  }
  
  /**
   * Gets input partitions for a stage based on its input sources.
   */
  private def getInputPartitionsForStage(
    stageInfo: StageBuilder.StageInfo, 
    stageResults: mutable.Map[Int, Seq[Partition[_]]],
    stageToShuffleId: mutable.Map[Int, Int]
  ): Seq[Partition[Any]] = {
    stageInfo.inputSources.flatMap {
      case StageBuilder.SourceInput(partitions) => 
        partitions.asInstanceOf[Seq[Partition[Any]]]
        
      case StageBuilder.ShuffleInput(plannedShuffleId, numPartitions) =>
        // Find the actual shuffle ID that was written (map from the stage that produced the data)
        val dependencies = stageInfo.id // current stage
        val sourceStages = stageToShuffleId.keys.filter(_ < stageInfo.id) // stages that ran before this one
        val actualShuffleId = sourceStages.headOption.flatMap(stageToShuffleId.get).getOrElse(plannedShuffleId)
        
        // Read shuffle data from ShuffleManager using actual shuffle ID
        (0 until numPartitions).map { partitionId =>
          ShuffleManager.readShufflePartition(actualShuffleId, partitionId).asInstanceOf[Partition[Any]]
        }
    }
  }
  
  /**
   * Executes a single stage by creating tasks and submitting them to TaskScheduler.
   */
  private def executeStage(
    stageInfo: StageBuilder.StageInfo, 
    inputPartitions: Seq[Partition[Any]]
  ): Seq[Partition[Any]] = {
    
    if (inputPartitions.isEmpty) {
      println(s"Warning: Stage ${stageInfo.id} has no input partitions")
      return Seq.empty
    }
    
    // Handle different types of stages
    stageInfo match {
      case info if info.isShuffleStage =>
        executeShuffleStage(info, inputPartitions)
      case info =>
        executeNarrowStage(info, inputPartitions)
    }
  }
  
  /**
   * Executes a narrow transformation stage (no shuffle).
   */
  private def executeNarrowStage(
    stageInfo: StageBuilder.StageInfo,
    inputPartitions: Seq[Partition[Any]]
  ): Seq[Partition[Any]] = {
    // Create tasks for each input partition
    val tasks = inputPartitions.map { partition =>
      Task.StageTask(partition, stageInfo.stage.asInstanceOf[Stage[Any, Any]])
    }
    
    // Execute tasks using TaskScheduler
    val results = TaskScheduler.submit(tasks)
    results.asInstanceOf[Seq[Partition[Any]]]
  }
  
  /**
   * Executes a shuffle stage by applying the appropriate shuffle operation.
   */
  private def executeShuffleStage(
    stageInfo: StageBuilder.StageInfo,
    inputPartitions: Seq[Partition[Any]]
  ): Seq[Partition[Any]] = {
    
    println(s"Executing shuffle stage ${stageInfo.id} with ${inputPartitions.size} input partitions")
    
    // Combine all partition data for shuffle operations
    val allData = inputPartitions.flatMap(_.data.asInstanceOf[Iterable[(Any, Any)]])
    
    // Apply the appropriate shuffle operation based on the Plan type
    val result = stageInfo.shuffleOperation match {
      case Some(Plan.GroupByKeyOp(_)) =>
        // GroupByKey: group values by key
        val groupedData = allData.groupBy(_._1).map { case (key, pairs) =>
          val values = pairs.map(_._2)
          (key, values)
        }
        groupedData.toSeq
        
      case Some(reduceByKey: Plan.ReduceByKeyOp[_, _]) =>
        // ReduceByKey: group by key, then reduce values
        val groupedData = allData.groupBy(_._1).map { case (key, pairs) =>
          val values = pairs.map(_._2)
          val reducedValue = values.reduceOption(reduceByKey.reduceFunc.asInstanceOf[(Any, Any) => Any])
            .getOrElse(throw new NoSuchElementException(s"No values found for key $key"))
          (key, reducedValue)
        }
        groupedData.toSeq
        
      case Some(sortBy: Plan.SortByOp[_, _]) =>
        // SortBy: simplified implementation - just return data as-is for now
        // Real implementation would sort by the key function with proper ordering
        allData
        
      case _ =>
        // Default: basic groupByKey logic
        val groupedData = allData.groupBy(_._1).map { case (key, pairs) =>
          val values = pairs.map(_._2)
          (key, values)
        }
        groupedData.toSeq
    }
    
    // Return as a single partition (simplified - real implementation would distribute across partitions)
    Seq(Partition(result))
  }
  
  /**
   * Handles shuffle output by storing data in ShuffleManager.
   * Returns the actual shuffle ID that was assigned.
   */
  private def handleShuffleOutput(
    stageInfo: StageBuilder.StageInfo,
    results: Seq[Partition[_]]
  ): Int = {
    // For key-value data, partition by key
    val keyValueResults = results.asInstanceOf[Seq[Partition[(Any, Any)]]]
    val shuffleData = ShuffleManager.partitionByKey(keyValueResults, results.size)
    val actualShuffleId = ShuffleManager.writeShuffleData(shuffleData)
    
    println(s"Stored shuffle data for stage ${stageInfo.id} with shuffle ID $actualShuffleId")
    actualShuffleId
  }
  
  /**
   * Checks if a plan requires DAG scheduling (contains shuffle operations).
   */
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