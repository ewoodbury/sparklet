package com.ewoodbury.sparklet.execution
import com.ewoodbury.sparklet.core.Plan

object Executor:
  /**
   * Translates a Plan into a sequence of executable Tasks.
   * This represents building the physical execution plan for a single stage.
   * * NOTE: This simplified version only handles a single stage of narrow transformations.
   * The full implementation will break the plan into a DAG of stages.
   */
  def createTasks[A](plan: Plan[A]): Seq[Task[_, A]] = {
    plan match {
      case com.ewoodbury.sparklet.core.Plan.MapOp(com.ewoodbury.sparklet.core.Plan.Source(partitions), f) =>
        partitions.map(p => Task.MapTask(p, f))

      case com.ewoodbury.sparklet.core.Plan.FilterOp(com.ewoodbury.sparklet.core.Plan.Source(partitions), p) =>
        partitions.map(part => Task.FilterTask(part, p))

      case com.ewoodbury.sparklet.core.Plan.FlatMapOp(com.ewoodbury.sparklet.core.Plan.Source(partitions), f) =>
        partitions.map(part => Task.FlatMapTask(part, f))

      case com.ewoodbury.sparklet.core.Plan.DistinctOp(com.ewoodbury.sparklet.core.Plan.Source(partitions)) =>
        partitions.map(part => Task.DistinctTask(part))

      case com.ewoodbury.sparklet.core.Plan.KeysOp(com.ewoodbury.sparklet.core.Plan.Source(partitions)) =>
        partitions.map(part => Task.KeysTask(part))

      case com.ewoodbury.sparklet.core.Plan.ValuesOp(com.ewoodbury.sparklet.core.Plan.Source(partitions)) =>
        partitions.map(part => Task.ValuesTask(part))

      case com.ewoodbury.sparklet.core.Plan.MapValuesOp(com.ewoodbury.sparklet.core.Plan.Source(partitions), f) =>
        partitions.map(part => Task.MapValuesTask(part, f))

      case com.ewoodbury.sparklet.core.Plan.FilterKeysOp(com.ewoodbury.sparklet.core.Plan.Source(partitions), p) =>
        partitions.map(part => Task.FilterKeysTask(part, p))

      case com.ewoodbury.sparklet.core.Plan.FilterValuesOp(com.ewoodbury.sparklet.core.Plan.Source(partitions), p) =>
        partitions.map(part => Task.FilterValuesTask(part, p))

      case com.ewoodbury.sparklet.core.Plan.FlatMapValuesOp(com.ewoodbury.sparklet.core.Plan.Source(partitions), f) =>
        partitions.map(part => Task.FlatMapValuesTask(part, f))

      case com.ewoodbury.sparklet.core.Plan.UnionOp(left, right) =>
        createTasks(left) ++ createTasks(right)

      case com.ewoodbury.sparklet.core.Plan.Source(_) =>
        throw new UnsupportedOperationException("Cannot create tasks from source directly")

      case _ =>
        throw new UnsupportedOperationException("Cannot create tasks from this plan")
    }
  }
end Executor