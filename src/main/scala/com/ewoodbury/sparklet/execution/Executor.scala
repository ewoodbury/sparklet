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
end Executor