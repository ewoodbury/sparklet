package com.ewoodbury.sparklet.core

/**
 * Centralized authority for determining whether Plan operations require shuffle boundaries.
 * 
 * This module consolidates shuffle detection logic that was previously scattered across
 * DAGScheduler, StageBuilder, and Operation modules, providing a single source of truth
 * for wide operation identification.
 */
object PlanWide {
  
  /**
   * Determines if a Plan operation requires a shuffle boundary (wide operation).
   * 
   * Wide operations are those that require data movement across partitions,
   * necessitating shuffle operations and DAG scheduling. This function recursively
   * traverses the plan tree to detect any wide operations.
   * 
   * @param plan The Plan node to check
   * @return true if the plan contains any wide operations, false otherwise
   */
  def isWide(plan: Plan[_]): Boolean = plan match {
    // Direct wide operations - all operations that require shuffle boundaries
    case _: Plan.GroupByKeyOp[_, _] | _: Plan.ReduceByKeyOp[_, _] | _: Plan.SortByOp[_, _] |
         _: Plan.JoinOp[_, _, _] | _: Plan.CoGroupOp[_, _, _] | _: Plan.RepartitionOp[_] |
         _: Plan.CoalesceOp[_] | _: Plan.PartitionByOp[_, _] =>
      true
    
    // Narrow operations - recurse on source(s) to check for downstream wide operations
    case Plan.MapOp(source, _) => isWide(source)
    case Plan.FilterOp(source, _) => isWide(source)
    case Plan.FlatMapOp(source, _) => isWide(source)
    case Plan.MapPartitionsOp(source, _) => isWide(source)
    case Plan.DistinctOp(source) => isWide(source)
    case Plan.KeysOp(source) => isWide(source)
    case Plan.ValuesOp(source) => isWide(source)
    case Plan.MapValuesOp(source, _) => isWide(source)
    case Plan.FilterKeysOp(source, _) => isWide(source)
    case Plan.FilterValuesOp(source, _) => isWide(source)
    case Plan.FlatMapValuesOp(source, _) => isWide(source)
    case Plan.UnionOp(left, right) => isWide(left) || isWide(right)
    
    // Base cases - sources are not wide
    case _: Plan.Source[_] => false
  }
  
  /**
   * Determines if a Plan operation is directly a wide operation (non-recursive).
   * 
   * Unlike `isWide`, this function only checks if the immediate operation requires
   * a shuffle boundary, without traversing the plan tree. Used for scenarios where
   * only the current operation's properties matter.
   * 
   * @param plan The Plan node to check
   * @return true if the plan itself is a wide operation, false otherwise
   */
  def isDirectlyWide(plan: Plan[_]): Boolean = plan match {
    case _: Plan.GroupByKeyOp[_, _] | _: Plan.ReduceByKeyOp[_, _] | _: Plan.SortByOp[_, _] |
         _: Plan.JoinOp[_, _, _] | _: Plan.CoGroupOp[_, _, _] | _: Plan.RepartitionOp[_] |
         _: Plan.CoalesceOp[_] | _: Plan.PartitionByOp[_, _] =>
      true
    case _ => false
  }
}
