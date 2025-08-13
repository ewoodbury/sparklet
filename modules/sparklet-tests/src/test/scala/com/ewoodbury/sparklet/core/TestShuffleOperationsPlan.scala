package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection

class TestShuffleOperationsPlan extends AnyFlatSpec with Matchers {
  val toDistCollection: [T] => (seq: Seq[T]) => DistCollection[T] = [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))

  "Shuffle transformations" should "create groupByKey transformations without triggering computation" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two", 3 -> "three"))
    val grouped = source.groupByKey
    
    // Should create the transformation successfully
    grouped.plan shouldBe a[Plan.GroupByKeyOp[_, _]]
    
    // Verify the source plan is preserved
    grouped.plan match {
      case Plan.GroupByKeyOp(sourcePlan) => sourcePlan shouldBe source.plan
      case _ => fail("Expected GroupByKeyOp")
    }
  }

  it should "create reduceByKey transformations without triggering computation" in {
    val source = toDistCollection(Seq("one" -> 1, "one" -> 1, "two" -> 2))
    val reduced = source.reduceByKey((a: Int, b: Int) => a + b)
    
    // Should create the transformation successfully  
    reduced.plan shouldBe a[Plan.ReduceByKeyOp[_, _]]
    
    // Verify the source plan is preserved
    reduced.plan match {
      case Plan.ReduceByKeyOp(sourcePlan, _) => sourcePlan shouldBe source.plan
      case _ => fail("Expected ReduceByKeyOp")
    }
  }

  it should "create sortBy transformations without triggering computation" in {
    val source = toDistCollection(Seq(3, 1, 4, 1, 5))
    val sorted = source.sortBy(identity)
    
    // Should create the transformation successfully
    sorted.plan shouldBe a[Plan.SortByOp[_, _]]
    
    // Verify the source plan is preserved
    sorted.plan match {
      case Plan.SortByOp(sourcePlan, _, _) => sourcePlan shouldBe source.plan
      case _ => fail("Expected SortByOp")
    }
  }

  it should "create join transformations without triggering computation" in {
    val left = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val right = toDistCollection(Seq(1 -> "uno", 2 -> "dos"))
    val joined = left.join(right)
    
    // Should create the transformation successfully
    joined.plan shouldBe a[Plan.JoinOp[_, _, _]]
    
    // Verify both source plans are preserved
    joined.plan match {
      case Plan.JoinOp(leftPlan, rightPlan) => 
        leftPlan shouldBe left.plan
        rightPlan shouldBe right.plan
      case _ => fail("Expected JoinOp")
    }
  }

  it should "create cogroup transformations without triggering computation" in {
    val left = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val right = toDistCollection(Seq(1 -> "uno", 2 -> "dos"))
    val cogrouped = left.cogroup(right)
    
    // Should create the transformation successfully
    cogrouped.plan shouldBe a[Plan.CoGroupOp[_, _, _]]
    
    // Verify both source plans are preserved
    cogrouped.plan match {
      case Plan.CoGroupOp(leftPlan, rightPlan) => 
        leftPlan shouldBe left.plan
        rightPlan shouldBe right.plan
      case _ => fail("Expected CoGroupOp")
    }
  }

  it should "create chained narrow-then-shuffle transformation plans correctly" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val mapped = source.map(x => (x._1 + 10, x._2.toUpperCase(java.util.Locale.ENGLISH)))
    val grouped = mapped.groupByKey
    
    // Should create the transformation chain correctly
    grouped.plan shouldBe a[Plan.GroupByKeyOp[_, _]]
    
    // Verify the structure
    grouped.plan match {
      case Plan.GroupByKeyOp(innerPlan) => 
        innerPlan shouldBe a[Plan.MapOp[_, _]]
      case _ => fail("Expected GroupByKeyOp")
    }
  }

  it should "create chained shuffle-then-narrow transformation plans correctly" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val grouped = source.groupByKey
    val mapped = grouped.map(_.toString)
    
    // Should create the transformation chain correctly
    mapped.plan shouldBe a[Plan.MapOp[_, _]]
    
    // Verify the structure
    mapped.plan match {
      case Plan.MapOp(innerPlan, _) => 
        innerPlan shouldBe a[Plan.GroupByKeyOp[_, _]]
      case _ => fail("Expected MapOp")
    }
  }
}


