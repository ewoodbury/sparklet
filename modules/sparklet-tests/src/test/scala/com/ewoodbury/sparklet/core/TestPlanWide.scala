package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Comprehensive unit tests for PlanWide module.
 * 
 * These tests ensure that the centralized wide operation detection logic correctly
 * identifies all wide operations and preserves the behavior of the original
 * scattered implementations.
 */
class TestPlanWide extends AnyFlatSpec with Matchers {

  // Test data setup
  private val source = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
  private val kvSource = Plan.MapOp(source, (x: Int) => (x, x.toString))
  private val kvSource2 = Plan.MapOp(source, (x: Int) => (x + 10, (x * 2).toString))

  "PlanWide.isWide" should "identify all direct wide operations" in {
    // Key-based shuffle operations
    PlanWide.isWide(Plan.GroupByKeyOp(kvSource)) shouldBe true
    PlanWide.isWide(Plan.ReduceByKeyOp(kvSource, (a: String, b: String) => a + b)) shouldBe true
    PlanWide.isWide(Plan.PartitionByOp(kvSource, 4)) shouldBe true
    
    // Global operations
    PlanWide.isWide(Plan.SortByOp(source, (x: Int) => x, Ordering.Int)) shouldBe true
    PlanWide.isWide(Plan.RepartitionOp(source, 4)) shouldBe true
    PlanWide.isWide(Plan.CoalesceOp(source, 2)) shouldBe true
    
    // Multi-input operations
    PlanWide.isWide(Plan.JoinOp(kvSource, kvSource2, None)) shouldBe true
    PlanWide.isWide(Plan.JoinOp(kvSource, kvSource2, Some(Plan.JoinStrategy.ShuffleHash))) shouldBe true
    PlanWide.isWide(Plan.JoinOp(kvSource, kvSource2, Some(Plan.JoinStrategy.SortMerge))) shouldBe true
    PlanWide.isWide(Plan.JoinOp(kvSource, kvSource2, Some(Plan.JoinStrategy.Broadcast))) shouldBe true
    PlanWide.isWide(Plan.CoGroupOp(kvSource, kvSource2)) shouldBe true
  }

  it should "identify narrow operations as non-wide" in {
    // Source operations
    PlanWide.isWide(source) shouldBe false
    
    // Single-input narrow operations
    PlanWide.isWide(Plan.MapOp(source, (x: Int) => x + 1)) shouldBe false
    PlanWide.isWide(Plan.FilterOp(source, (x: Int) => x > 0)) shouldBe false
    PlanWide.isWide(Plan.FlatMapOp(source, (x: Int) => Seq(x, x + 1))) shouldBe false
    PlanWide.isWide(Plan.MapPartitionsOp(source, (iter: Iterator[Int]) => iter.map(_ * 2))) shouldBe false
    PlanWide.isWide(Plan.DistinctOp(source)) shouldBe false
    
    // Key-value narrow operations
    PlanWide.isWide(Plan.KeysOp(kvSource)) shouldBe false
    PlanWide.isWide(Plan.ValuesOp(kvSource)) shouldBe false
    PlanWide.isWide(Plan.MapValuesOp(kvSource, (s: String) => s.toUpperCase(java.util.Locale.ENGLISH))) shouldBe false
    PlanWide.isWide(Plan.FilterKeysOp(kvSource, (k: Int) => k > 0)) shouldBe false
    PlanWide.isWide(Plan.FilterValuesOp(kvSource, (v: String) => v.nonEmpty)) shouldBe false
    PlanWide.isWide(Plan.FlatMapValuesOp(kvSource, (s: String) => Seq(s, s + "!"))) shouldBe false
    
    // Multi-input narrow operations
    PlanWide.isWide(Plan.UnionOp(source, source)) shouldBe false
  }

  it should "recursively detect wide operations in narrow operation trees" in {
    val wideOp = Plan.GroupByKeyOp(kvSource)
    
    // Single level of narrow operations over wide
    PlanWide.isWide(Plan.MapOp(wideOp, identity)) shouldBe true
    PlanWide.isWide(Plan.FilterOp(wideOp, (_: (Int, Iterable[String])) => true)) shouldBe true
    PlanWide.isWide(Plan.FlatMapOp(wideOp, (x: (Int, Iterable[String])) => Seq(x))) shouldBe true
    
    // Multiple levels of narrow operations over wide
    val nestedNarrow = Plan.MapOp(
      Plan.FilterOp(
        Plan.MapOp(wideOp, identity),
        (_: (Int, Iterable[String])) => true
      ),
      identity
    )
    PlanWide.isWide(nestedNarrow) shouldBe true
  }

  it should "handle union operations correctly" in {
    val narrowLeft = Plan.MapOp(source, (x: Int) => x + 1)
    val narrowRight = Plan.FilterOp(source, (x: Int) => x > 0)
    val wideLeft = Plan.SortByOp(source, identity, Ordering.Int)
    val wideRight = Plan.RepartitionOp(source, 4) // Use RepartitionOp to keep same type as source
    
    // Both narrow - should be narrow
    PlanWide.isWide(Plan.UnionOp(narrowLeft, narrowRight)) shouldBe false
    
    // Left wide, right narrow - should be wide
    PlanWide.isWide(Plan.UnionOp(wideLeft, narrowRight)) shouldBe true
    
    // Left narrow, right wide - should be wide  
    PlanWide.isWide(Plan.UnionOp(narrowLeft, wideRight)) shouldBe true
    
    // Both wide - should be wide
    PlanWide.isWide(Plan.UnionOp(wideLeft, wideRight)) shouldBe true
  }

  it should "handle deeply nested plans correctly" in {
    // Create a deeply nested plan with wide operation at the bottom
    val deepWide = Plan.MapOp(
      Plan.FilterOp(
        Plan.FlatMapOp(
          Plan.MapOp(
            Plan.GroupByKeyOp(kvSource), // Wide operation at the bottom
            identity
          ),
          (x: (Int, Iterable[String])) => Seq(x)
        ),
        (_: (Int, Iterable[String])) => true
      ),
      identity
    )
    PlanWide.isWide(deepWide) shouldBe true
    
    // Create a deeply nested plan with no wide operations
    val deepNarrow = Plan.MapOp(
      Plan.FilterOp(
        Plan.FlatMapOp(
          Plan.MapOp(
            source, // No wide operations
            (x: Int) => x + 1
          ),
          (x: Int) => Seq(x, x + 1)
        ),
        (x: Int) => x > 0
      ),
      (x: Int) => x * 2
    )
    PlanWide.isWide(deepNarrow) shouldBe false
  }

  "PlanWide.isDirectlyWide" should "only check the immediate operation" in {
    val wideOp = Plan.GroupByKeyOp(kvSource)
    val narrowOverWide = Plan.MapOp(wideOp, identity)
    
    // The wide operation itself should be directly wide
    PlanWide.isDirectlyWide(wideOp) shouldBe true
    
    // The narrow operation over wide should not be directly wide
    PlanWide.isDirectlyWide(narrowOverWide) shouldBe false
    
    // But isWide should still detect the underlying wide operation
    PlanWide.isWide(narrowOverWide) shouldBe true
  }

  it should "identify all direct wide operation types" in {
    // Test each wide operation type individually
    PlanWide.isDirectlyWide(Plan.GroupByKeyOp(kvSource)) shouldBe true
    PlanWide.isDirectlyWide(Plan.ReduceByKeyOp(kvSource, (a: String, b: String) => a + b)) shouldBe true
    PlanWide.isDirectlyWide(Plan.SortByOp(source, identity, Ordering.Int)) shouldBe true
    PlanWide.isDirectlyWide(Plan.PartitionByOp(kvSource, 4)) shouldBe true
    PlanWide.isDirectlyWide(Plan.RepartitionOp(source, 4)) shouldBe true
    PlanWide.isDirectlyWide(Plan.CoalesceOp(source, 2)) shouldBe true
    PlanWide.isDirectlyWide(Plan.JoinOp(kvSource, kvSource2, None)) shouldBe true
    PlanWide.isDirectlyWide(Plan.CoGroupOp(kvSource, kvSource2)) shouldBe true
  }

  it should "not identify narrow operations as directly wide" in {
    // Test each narrow operation type individually
    PlanWide.isDirectlyWide(source) shouldBe false
    PlanWide.isDirectlyWide(Plan.MapOp(source, _ + 1)) shouldBe false
    PlanWide.isDirectlyWide(Plan.FilterOp(source, _ > 0)) shouldBe false
    PlanWide.isDirectlyWide(Plan.FlatMapOp(source, x => Seq(x))) shouldBe false
    PlanWide.isDirectlyWide(Plan.MapPartitionsOp(source, identity)) shouldBe false
    PlanWide.isDirectlyWide(Plan.DistinctOp(source)) shouldBe false
    PlanWide.isDirectlyWide(Plan.KeysOp(kvSource)) shouldBe false
    PlanWide.isDirectlyWide(Plan.ValuesOp(kvSource)) shouldBe false
    PlanWide.isDirectlyWide(Plan.MapValuesOp(kvSource, identity)) shouldBe false
    PlanWide.isDirectlyWide(Plan.FilterKeysOp(kvSource, _ => true)) shouldBe false
    PlanWide.isDirectlyWide(Plan.FilterValuesOp(kvSource, _ => true)) shouldBe false
    PlanWide.isDirectlyWide(Plan.FlatMapValuesOp(kvSource, x => Seq(x))) shouldBe false
    PlanWide.isDirectlyWide(Plan.UnionOp(source, source)) shouldBe false
  }

  "PlanWide functions" should "handle edge cases" in {
    // Empty partitions should still work
    val emptySource = Plan.Source(Seq.empty[Partition[Int]])
    PlanWide.isWide(emptySource) shouldBe false
    PlanWide.isDirectlyWide(emptySource) shouldBe false
    
    // Wide operations over empty sources should still be wide
    val emptyKvSource = Plan.MapOp(emptySource, (x: Int) => (x, x))
    PlanWide.isWide(Plan.GroupByKeyOp(emptyKvSource)) shouldBe true
    PlanWide.isDirectlyWide(Plan.GroupByKeyOp(emptyKvSource)) shouldBe true
  }

  "PlanWide.isWide and isDirectlyWide" should "be consistent" in {
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
    
    // For any directly narrow operation, both should return false (unless it has wide children)
    val narrowOperations = Seq(
      source,
      Plan.MapOp(source, _ + 1),
      Plan.FilterOp(source, _ > 0),
      Plan.KeysOp(kvSource),
      Plan.ValuesOp(kvSource)
    )
    
    narrowOperations.foreach { op =>
      withClue(s"Operation: $op") {
        PlanWide.isDirectlyWide(op) shouldBe false
        PlanWide.isWide(op) shouldBe false
      }
    }
  }
}
