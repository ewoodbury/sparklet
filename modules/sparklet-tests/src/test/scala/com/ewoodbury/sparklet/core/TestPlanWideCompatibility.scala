package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.ewoodbury.sparklet.execution.DAGScheduler

/**
 * Compatibility tests to ensure the new PlanWide implementation matches
 * the behavior of existing DAGScheduler implementation.
 * 
 * These tests verify that the new centralized logic produces the same results
 * as the original DAGScheduler implementation for all operation types.
 * 
 * TODO: Once fully validated, and DAGScheduler logic is removed, this test class
 * can be deleted as it will no longer serve a purpose.
 */
class TestPlanWideCompatibility extends AnyFlatSpec with Matchers {

  // Test data setup
  private val source = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
  private val kvSource = Plan.MapOp(source, (x: Int) => (x, x.toString))
  private val kvSource2 = Plan.MapOp(source, (x: Int) => (x + 10, (x * 2).toString))

  "PlanWide.isWide" should "match DAGScheduler.requiresDAGScheduling for all wide operations" in {
    val wideOperations = Seq(
      Plan.GroupByKeyOp(kvSource),
      Plan.ReduceByKeyOp(kvSource, (a: String, b: String) => a + b),
      Plan.SortByOp(source, (x: Int) => x, Ordering.Int),
      Plan.PartitionByOp(kvSource, 4),
      Plan.RepartitionOp(source, 4),
      Plan.CoalesceOp(source, 2),
      Plan.JoinOp(kvSource, kvSource2, None),
      Plan.JoinOp(kvSource, kvSource2, Some(Plan.JoinStrategy.ShuffleHash)),
      Plan.JoinOp(kvSource, kvSource2, Some(Plan.JoinStrategy.SortMerge)),
      Plan.JoinOp(kvSource, kvSource2, Some(Plan.JoinStrategy.Broadcast)),
      Plan.CoGroupOp(kvSource, kvSource2)
    )
    
    wideOperations.foreach { plan =>
      withClue(s"Plan: $plan") {
        val dagSchedulerResult = DAGScheduler.requiresDAGScheduling(plan)
        val planWideResult = PlanWide.isWide(plan)
        planWideResult shouldBe dagSchedulerResult
        
        // All these operations should be detected as wide
        planWideResult shouldBe true
      }
    }
  }

  it should "match DAGScheduler.requiresDAGScheduling for all narrow operations" in {
    val narrowOperations = Seq(
      source,
      Plan.MapOp(source, (x: Int) => x + 1),
      Plan.FilterOp(source, (x: Int) => x > 0),
      Plan.FlatMapOp(source, (x: Int) => Seq(x, x + 1)),
      Plan.MapPartitionsOp(source, (iter: Iterator[Int]) => iter.map(_ * 2)),
      Plan.DistinctOp(source),
      Plan.KeysOp(kvSource),
      Plan.ValuesOp(kvSource),
      Plan.MapValuesOp(kvSource, (s: String) => s.toUpperCase(java.util.Locale.ENGLISH)),
      Plan.FilterKeysOp(kvSource, (k: Int) => k > 0),
      Plan.FilterValuesOp(kvSource, (v: String) => v.nonEmpty),
      Plan.FlatMapValuesOp(kvSource, (s: String) => Seq(s, s + "!")),
      Plan.UnionOp(source, source)
    )
    
    narrowOperations.foreach { plan =>
      withClue(s"Plan: $plan") {
        val dagSchedulerResult = DAGScheduler.requiresDAGScheduling(plan)
        val planWideResult = PlanWide.isWide(plan)
        planWideResult shouldBe dagSchedulerResult
        
        // All these operations should be detected as narrow
        planWideResult shouldBe false
      }
    }
  }

  it should "match DAGScheduler.requiresDAGScheduling for nested operations" in {
    val nestedOperations = Seq(
      // Narrow over wide
      Plan.MapOp(Plan.GroupByKeyOp(kvSource), identity),
      Plan.FilterOp(Plan.SortByOp(source, identity, Ordering.Int), (_: Int) => true),
      
      // Wide over narrow
      Plan.GroupByKeyOp(Plan.MapOp(kvSource, identity)),
      Plan.RepartitionOp(Plan.FilterOp(source, _ > 0), 4),
      
      // Deep nesting
      Plan.MapOp(
        Plan.FilterOp(
          Plan.GroupByKeyOp(kvSource),
          (_: (Int, Iterable[String])) => true
        ),
        identity
      )
    )
    
    nestedOperations.foreach { plan =>
      withClue(s"Plan: $plan") {
        val dagSchedulerResult = DAGScheduler.requiresDAGScheduling(plan)
        val planWideResult = PlanWide.isWide(plan)
        planWideResult shouldBe dagSchedulerResult
      }
    }
  }

  it should "match DAGScheduler.requiresDAGScheduling for union operations" in {
    val narrow1 = Plan.MapOp(source, (x: Int) => x + 1)
    val narrow2 = Plan.FilterOp(source, (x: Int) => x > 0)
    val wide1 = Plan.SortByOp(source, identity, Ordering.Int)
    val wide2 = Plan.RepartitionOp(source, 4)
    
    val unionOperations = Seq(
      Plan.UnionOp(narrow1, narrow2), // Should be narrow
      Plan.UnionOp(narrow1, wide1),   // Should be wide
      Plan.UnionOp(wide1, narrow2),   // Should be wide  
      Plan.UnionOp(wide1, wide2)      // Should be wide
    )
    
    unionOperations.foreach { plan =>
      withClue(s"Plan: $plan") {
        val dagSchedulerResult = DAGScheduler.requiresDAGScheduling(plan)
        val planWideResult = PlanWide.isWide(plan)
        planWideResult shouldBe dagSchedulerResult
      }
    }
  }

  "PlanWide.isDirectlyWide" should "correctly identify directly wide operations" in {
    // Operations that should be directly wide
    val directlyWideOperations = Seq(
      Plan.GroupByKeyOp(kvSource),
      Plan.ReduceByKeyOp(kvSource, (a: String, b: String) => a + b),
      Plan.SortByOp(source, (x: Int) => x, Ordering.Int),
      Plan.PartitionByOp(kvSource, 4),
      Plan.RepartitionOp(source, 4),
      Plan.CoalesceOp(source, 2),
      Plan.JoinOp(kvSource, kvSource2, None),
      Plan.CoGroupOp(kvSource, kvSource2)
    )
    
    directlyWideOperations.foreach { plan =>
      withClue(s"Plan: $plan") {
        PlanWide.isDirectlyWide(plan) shouldBe true
      }
    }
    
    // Operations that should not be directly wide
    val notDirectlyWideOperations = Seq(
      source,
      Plan.MapOp(source, (x: Int) => x + 1),
      Plan.FilterOp(source, (x: Int) => x > 0),
      Plan.FlatMapOp(source, (x: Int) => Seq(x, x + 1)),
      Plan.MapPartitionsOp(source, (iter: Iterator[Int]) => iter.map(_ * 2)),
      Plan.DistinctOp(source),
      Plan.KeysOp(kvSource),
      Plan.ValuesOp(kvSource),
      Plan.MapValuesOp(kvSource, (s: String) => s.toUpperCase(java.util.Locale.ENGLISH)),
      Plan.FilterKeysOp(kvSource, (k: Int) => k > 0),
      Plan.FilterValuesOp(kvSource, (v: String) => v.nonEmpty),
      Plan.FlatMapValuesOp(kvSource, (s: String) => Seq(s, s + "!")),
      Plan.UnionOp(source, source),
      Plan.MapOp(Plan.GroupByKeyOp(kvSource), identity) // Narrow over wide
    )
    
    notDirectlyWideOperations.foreach { plan =>
      withClue(s"Plan: $plan") {
        PlanWide.isDirectlyWide(plan) shouldBe false
      }
    }
  }

  "PlanWide.isWide vs DAGScheduler.requiresDAGScheduling" should "produce identical results for all operations" in {
    val allOperations = Seq(
      // Wide operations
      Plan.GroupByKeyOp(kvSource),
      Plan.ReduceByKeyOp(kvSource, (a: String, b: String) => a + b),
      Plan.SortByOp(source, (x: Int) => x, Ordering.Int),
      Plan.PartitionByOp(kvSource, 4),
      Plan.RepartitionOp(source, 4),
      Plan.CoalesceOp(source, 2),
      Plan.JoinOp(kvSource, kvSource2, None),
      Plan.JoinOp(kvSource, kvSource2, Some(Plan.JoinStrategy.ShuffleHash)),
      Plan.JoinOp(kvSource, kvSource2, Some(Plan.JoinStrategy.SortMerge)),
      Plan.JoinOp(kvSource, kvSource2, Some(Plan.JoinStrategy.Broadcast)),
      Plan.CoGroupOp(kvSource, kvSource2),
      
      // Narrow operations
      source,
      Plan.MapOp(source, (x: Int) => x + 1),
      Plan.FilterOp(source, (x: Int) => x > 0),
      Plan.FlatMapOp(source, (x: Int) => Seq(x, x + 1)),
      Plan.MapPartitionsOp(source, (iter: Iterator[Int]) => iter.map(_ * 2)),
      Plan.DistinctOp(source),
      Plan.KeysOp(kvSource),
      Plan.ValuesOp(kvSource),
      Plan.MapValuesOp(kvSource, (s: String) => s.toUpperCase(java.util.Locale.ENGLISH)),
      Plan.FilterKeysOp(kvSource, (k: Int) => k > 0),
      Plan.FilterValuesOp(kvSource, (v: String) => v.nonEmpty),
      Plan.FlatMapValuesOp(kvSource, (s: String) => Seq(s, s + "!")),
      Plan.UnionOp(source, source),
      
      // Nested operations
      Plan.MapOp(Plan.GroupByKeyOp(kvSource), identity),
      Plan.FilterOp(Plan.SortByOp(source, identity, Ordering.Int), (_: Int) => true),
      Plan.GroupByKeyOp(Plan.MapOp(kvSource, identity)),
      Plan.RepartitionOp(Plan.FilterOp(source, _ > 0), 4)
    )
    
    allOperations.foreach { plan =>
      withClue(s"Plan: $plan") {
        val dagSchedulerResult = DAGScheduler.requiresDAGScheduling(plan)
        val planWideResult = PlanWide.isWide(plan)
        planWideResult shouldBe dagSchedulerResult
      }
    }
  }

  "PlanWide functions" should "be consistent with each other" in {
    // For any directly wide operation, isWide should also return true
    val wideOperations = Seq(
      Plan.GroupByKeyOp(kvSource),
      Plan.ReduceByKeyOp(kvSource, (a: String, b: String) => a + b),
      Plan.SortByOp(source, identity, Ordering.Int),
      Plan.PartitionByOp(kvSource, 4),
      Plan.RepartitionOp(source, 4),
      Plan.CoalesceOp(source, 2),
      Plan.JoinOp(kvSource, kvSource2, None),
      Plan.CoGroupOp(kvSource, kvSource2)
    )
    
    wideOperations.foreach { op =>
      withClue(s"Operation: $op") {
        PlanWide.isDirectlyWide(op) shouldBe true
        PlanWide.isWide(op) shouldBe true
      }
    }
    
    // For narrow operations over wide operations, only isWide should return true
    val narrowOverWide = Plan.MapOp(Plan.GroupByKeyOp(kvSource), identity)
    PlanWide.isDirectlyWide(narrowOverWide) shouldBe false
    PlanWide.isWide(narrowOverWide) shouldBe true
  }
}
