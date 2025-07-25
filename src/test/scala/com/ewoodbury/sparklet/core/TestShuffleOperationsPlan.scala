package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestShuffleOperations extends AnyFlatSpec with Matchers {
  val toDistCollection = [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))

  "Shuffle transformations" should "create groupByKey transformations without triggering computation" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two", 3 -> "three"))
    val grouped = source.groupByKey
    
    // Should create the transformation successfully
    grouped.plan shouldBe a[Plan.GroupByKeyOp[_, _]]
  }

  it should "create reduceByKey transformations without triggering computation" in {
    val source = toDistCollection(Seq("one" -> 1, "one" -> 1, "two" -> 2))
    val reduced = source.reduceByKey((a: Int, b: Int) => a + b)
    
    // Should create the transformation successfully  
    reduced.plan shouldBe a[Plan.ReduceByKeyOp[_, _]]
  }

  it should "create sortBy transformations without triggering computation" in {
    val source = toDistCollection(Seq(3, 1, 4, 1, 5))
    val sorted = source.sortBy(identity)
    
    // Should create the transformation successfully
    sorted.plan shouldBe a[Plan.SortByOp[_, _]]
  }

  it should "create join transformations without triggering computation" in {
    val left = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val right = toDistCollection(Seq(1 -> "uno", 2 -> "dos"))
    val joined = left.join(right)
    
    // Should create the transformation successfully
    joined.plan shouldBe a[Plan.JoinOp[_, _, _]]
  }

  it should "create cogroup transformations without triggering computation" in {
    val left = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val right = toDistCollection(Seq(1 -> "uno", 2 -> "dos"))
    val cogrouped = left.cogroup(right)
    
    // Should create the transformation successfully
    cogrouped.plan shouldBe a[Plan.CoGroupOp[_, _, _]]
  }

  it should "throw UnsupportedOperationException when trying to collect groupByKey" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val grouped = source.groupByKey
    
    assertThrows[UnsupportedOperationException] {
      grouped.collect()
    }
  }

  it should "throw UnsupportedOperationException when trying to collect reduceByKey" in {
    val source = toDistCollection(Seq("one" -> 1, "one" -> 1, "two" -> 2))
    val reduced = source.reduceByKey((a: Int, b: Int) => a + b)
    
    assertThrows[UnsupportedOperationException] {
      reduced.collect()
    }
  }

  it should "throw UnsupportedOperationException when trying to collect sortBy" in {
    val source = toDistCollection(Seq(3, 1, 4, 1, 5))
    val sorted = source.sortBy(identity)
    
    assertThrows[UnsupportedOperationException] {
      sorted.collect()
    }
  }

  it should "allow chaining narrow transformations after shuffle operations" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val chained = source.groupByKey.map(_.toString)
    
    // Should create the chained transformation successfully
    chained.plan shouldBe a[Plan.MapOp[_, _]]
    
    // But still throw when trying to collect due to the shuffle
    assertThrows[UnsupportedOperationException] {
      chained.collect()
    }
  }
}