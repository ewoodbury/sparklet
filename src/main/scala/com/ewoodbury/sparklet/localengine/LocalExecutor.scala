package com.ewoodbury.sparklet.localengine

object LocalExecutor:
  /**
   * Translates a Plan into a sequence of executable Tasks.
   * This represents building the physical execution plan for a single stage.
   * * NOTE: This simplified version only handles a single stage of narrow transformations.
   * The full implementation will break the plan into a DAG of stages.
   */
  def createTasks[A](plan: Plan[A]): Seq[Task[_, A]] = {
    plan match {
      case Plan.MapOp(Plan.Source(partitions), f) =>
        partitions.map(p => Task.MapTask(p, f))

      case Plan.FilterOp(Plan.Source(partitions), p) =>
        partitions.map(part => Task.FilterTask(part, p))

      case Plan.FlatMapOp(Plan.Source(partitions), f) =>
        partitions.map(part => Task.FlatMapTask(part, f))

      case Plan.DistinctOp(Plan.Source(partitions)) =>
        partitions.map(part => Task.DistinctTask(part))

      case Plan.KeysOp(Plan.Source(partitions)) =>
        partitions.map(part => Task.KeysTask(part))

      case Plan.ValuesOp(Plan.Source(partitions)) =>
        partitions.map(part => Task.ValuesTask(part))

      case Plan.MapValuesOp(Plan.Source(partitions), f) =>
        partitions.map(part => Task.MapValuesTask(part, f))

      case Plan.FilterKeysOp(Plan.Source(partitions), p) =>
        partitions.map(part => Task.FilterKeysTask(part, p))

      case Plan.FilterValuesOp(Plan.Source(partitions), p) =>
        partitions.map(part => Task.FilterValuesTask(part, p))

      case Plan.FlatMapValuesOp(Plan.Source(partitions), f) =>
        partitions.map(part => Task.FlatMapValuesTask(part, f))

      case Plan.UnionOp(left, right) =>
        createTasks(left) ++ createTasks(right)

      case Plan.Source(_) =>
        throw new UnsupportedOperationException("Cannot create tasks from source directly")

      case _ =>
        throw new UnsupportedOperationException("Cannot create tasks from this plan")
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

      case Plan.FilterValuesOp(source, p) =>
        println(" -> Computing FilterValuesOp")
        val inputPartitions = compute(source)
        inputPartitions.map(part => Partition(part.data.filter { case (_, v) => p(v) }))

      case Plan.FlatMapValuesOp(source, f) =>
        println(" -> Computing FlatMapValuesOp")
        val inputPartitions = compute(source)
        inputPartitions.map(part => Partition(part.data.flatMap { case (k, v) => f(v).map(b => (k, b)) }))
    }
  }
end LocalExecutor