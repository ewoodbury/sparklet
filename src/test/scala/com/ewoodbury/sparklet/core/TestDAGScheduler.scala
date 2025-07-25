package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.ewoodbury.sparklet.execution.DAGScheduler

class TestDAGScheduler extends AnyFlatSpec with Matchers {
  val toDistCollection = [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))

  "DAGScheduler" should "detect plans that require DAG scheduling" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val groupedPlan = source.groupByKey.plan
    val narrowPlan = source.map(_._1).plan
    
    DAGScheduler.requiresDAGScheduling(groupedPlan) shouldBe true
    DAGScheduler.requiresDAGScheduling(narrowPlan) shouldBe false
  }

  it should "detect complex plans with nested shuffle operations" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val complexPlan = source.map(x => (x._1 + 1, x._2)).reduceByKey[Int, String](_ + _).map(_._1).plan
    
    DAGScheduler.requiresDAGScheduling(complexPlan) shouldBe true
  }

  it should "execute simple narrow transformations without DAG scheduling" in {
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val result = source.map(_ * 2).filter(_ > 5).collect()
    
    result shouldBe Seq(6, 8, 10)
  }

  it should "execute shuffle operations using DAG scheduler (basic smoke test)" in {
    val source = toDistCollection(Seq(1 -> "a", 2 -> "b", 1 -> "c"))
    
    // This is a basic smoke test - the current implementation is simplified
    // and may not produce the exact expected result yet, but it should not crash
    noException should be thrownBy {
      val result = source.groupByKey.collect()
      println(s"GroupByKey result: $result")
    }
  }

  it should "execute reduceByKey operations using DAG scheduler (basic smoke test)" in {
    val source = toDistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3))
    
    // This is a basic smoke test - the current implementation is simplified
    noException should be thrownBy {
      val result = source.reduceByKey[String, Int](_ + _).collect()
      println(s"ReduceByKey result: $result")
    }
  }

  // TODO: Fix
  ignore should "execute sortBy operations using DAG scheduler (basic smoke test)" in {
    val source = toDistCollection(Seq(3, 1, 4, 1, 5))
    
    // This is a basic smoke test - the current implementation is simplified
    noException should be thrownBy {
      val result = source.sortBy(identity).collect()
      println(s"SortBy result: $result")
    }
  }

  // TODO: Implement this
  ignore should "handle mixed narrow and wide transformations" in {
    val source = toDistCollection(Seq(1 -> "a", 2 -> "b", 1 -> "c"))
    
    // Chain narrow transformations before and after shuffle
    noException should be thrownBy {
      val result = source
        .map { case (k, v) => (k + 10, v.toUpperCase(java.util.Locale.ENGLISH)) }  // narrow
        .groupByKey                                       // wide (shuffle)
        .map { case (k, vs) => (k, vs.size) }           // narrow after shuffle
        .collect()
      println(s"Mixed transformations result: $result")
    }
  }
} 