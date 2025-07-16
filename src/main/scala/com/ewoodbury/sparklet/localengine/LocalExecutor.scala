package com.ewoodbury.sparklet.localengine

object LocalExecutor:
  /**
   * Translates a Plan into a sequence of executable Tasks.
   * This represents building the physical execution plan for a single stage.
   * * NOTE: This simplified version only handles a single stage of narrow transformations.
   * A full implementation would break the plan into a DAG of stages.
   */
  def createTasks[A](plan: Plan[A]): Seq[Task[_, A]] = {
    plan match {
      // The `compute` method is now a private helper to get parent partitions
      // This part is a bit tricky, so we'll simplify for now. In a real system,
      // you'd pass partition data or locations between stages.
      case Plan.MapOp(source, f) =>
        val inputPartitions = LocalExecutor.compute(source) // Get parent partitions
        inputPartitions.map(p => Task.MapTask(p, f))

      case Plan.FilterOp(source, predicate) =>
        val inputPartitions = LocalExecutor.compute(source)
        inputPartitions.map(partition => Task.FilterTask(partition, predicate))
        
      // TODO: Add other narrow transformations here

      // Base case still needs to be handled:
      case Plan.Source(partitions) =>
        // A source doesn't have tasks, it's just data. We return a "pre-computed" result.
        // This suggests we need a slightly different model, which leads to Stages.
        // For now, let's adjust the collect method to handle this.
        throw new UnsupportedOperationException("Cannot create tasks from a source directly. The scheduler needs a stage.")
    }
  }

  /**
   * Executes a Plan by computing its resulting partitions.
   * This is the core of our "proto-distributed" engine. For narrow transformations,
   * it processes each input partition independently to produce an output partition.
   *
   * @param plan The logical plan to execute.
   * @return A sequence of Partitions representing the result.
   */
  def compute[A](plan: Plan[A]): Seq[Partition[A]] = {
    println(s"Computing partitions for plan node: $plan")
    plan match {
      // Base case: The Source plan just provides the initial partitions.
      case Plan.Source(partitions) =>
        println(s" -> Reached Source with ${partitions.length} partitions.")
        partitions

      // Narrow transformations:
      // Process each partition independently.
      case Plan.MapOp(source, f) =>
        println(" -> Computing MapOp")
        val inputPartitions = compute(source) // Recursively compute parent partitions
        val resultPartitions = inputPartitions.map { part =>
          // Apply the function to the data within each partition
          Partition(part.data.map(f))
        }
        println(s" -> MapOp produced ${resultPartitions.length} new partitions.")
        resultPartitions

      case Plan.FilterOp(source, p) =>
        println(" -> Computing FilterOp")
        val inputPartitions = compute(source)
        val resultPartitions = inputPartitions.map { part =>
          Partition(part.data.filter(p))
        }
        println(s" -> FilterOp produced ${resultPartitions.length} new partitions.")
        resultPartitions
        
      case Plan.FlatMapOp(source, f) =>
        println(" -> Computing FlatMapOp")
        val inputPartitions = compute(source)
        val resultPartitions = inputPartitions.map { part =>
          Partition(part.data.flatMap(f))
        }
        println(s" -> FlatMapOp produced ${resultPartitions.length} new partitions.")
        resultPartitions

      // NOTE: Wide transformations like distinct and union are more complex.
      // A true distributed implementation requires a shuffle.
      // For now, we simulate it by collecting all data to the "driver" (here),
      // performing the operation, and then re-partitioning into a single partition.

      case Plan.DistinctOp(source) =>
        println(" -> Computing DistinctOp (via collect-and-repartition)")
        val allData = compute(source).flatMap(_.data)
        val distinctData = allData.distinct
        Seq(Partition(distinctData))

      case Plan.UnionOp(left, right) =>
        println(" -> Computing UnionOp")
        val leftPartitions = compute(left)
        val rightPartitions = compute(right)
        leftPartitions ++ rightPartitions
        
      // --- Key-Value Transformations (all narrow) ---
      
      case Plan.KeysOp(source) =>
        println(" -> Computing KeysOp")
        val inputPartitions = compute(source)
        inputPartitions.map(part => Partition(part.data.map(_._1)))

      case Plan.ValuesOp(source) =>
        println(" -> Computing ValuesOp")
        val inputPartitions = compute(source)
        inputPartitions.map(part => Partition(part.data.map(_._2)))

      case Plan.MapValuesOp(source, f) =>
        println(" -> Computing MapValuesOp")
        val inputPartitions = compute(source)
        inputPartitions.map(part => Partition(part.data.map { case (k, v) => (k, f(v)) }))

      case Plan.FilterKeysOp(source, p) =>
        println(" -> Computing FilterKeysOp")
        val inputPartitions = compute(source)
        inputPartitions.map(part => Partition(part.data.filter { case (k, _) => p(k) }))

      case Plan.FlatMapValuesOp(source, f) =>
        println(" -> Computing FlatMapValuesOp")
        val inputPartitions = compute(source)
        inputPartitions.map(part => Partition(part.data.flatMap { case (k, v) => f(v).map(b => (k, b)) }))
    }
  }
end LocalExecutor