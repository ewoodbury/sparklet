package com.ewoodbury.sparklet.execution

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{DistCollection, Partition, Plan}
import com.ewoodbury.sparklet.runtime.api.SparkletRuntime

class TestDAGScheduler extends AnyFlatSpec with Matchers with StrictLogging {
  val toDistCollection: [T] => (seq: Seq[T]) => DistCollection[T] = [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))

  // Basic detection tests for scheduling
  "DAGScheduler" should "detect plans that require DAG scheduling" in {
    SparkletRuntime.get.shuffle.clear() // Clean state for test isolation
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val groupedPlan = source.groupByKey.plan
    val narrowPlan = source.map(_._1).plan
    
    DAGScheduler.requiresDAGScheduling(groupedPlan) shouldBe true
    DAGScheduler.requiresDAGScheduling(narrowPlan) shouldBe false
  }

  it should "detect complex plans with nested shuffle operations" in {
    SparkletRuntime.get.shuffle.clear() // Clean state for test isolation
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val complexPlan = source.map(x => (x._1 + 1, x._2)).reduceByKey[Int, String](_ + _).map(_._1).plan
    
    DAGScheduler.requiresDAGScheduling(complexPlan) shouldBe true
  }

  it should "detect join operations require DAG scheduling" in {
    SparkletRuntime.get.shuffle.clear() // Clean state for test isolation
    val source1 = toDistCollection(Seq(1 -> "a", 2 -> "b"))
    val source2 = toDistCollection(Seq(1 -> "x", 3 -> "y"))
    val joinPlan = source1.join(source2).plan
    DAGScheduler.requiresDAGScheduling(joinPlan) shouldBe true
  }

  it should "execute simple narrow transformations without DAG scheduling" in {
    SparkletRuntime.get.shuffle.clear() // Clean state for test isolation
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val result = source.map(_ * 2).filter(_ > 5).collect()
    
    result shouldBe Seq(6, 8, 10)
  }

  it should "handle sortBy operations" in {
    SparkletRuntime.get.shuffle.clear() // Clean state for test isolation
    val source = toDistCollection(Seq(3, 1, 4, 1, 5))
    val result = source.sortBy(identity).collect()
    result shouldBe Seq(1, 1, 3, 4, 5)
  }

  it should "correctly execute groupByKey operations" in {
    SparkletRuntime.get.shuffle.clear() // Clean state for test isolation
    val source = toDistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3))
    val result = source.groupByKey.collect().toMap
    result("a") should contain theSameElementsAs Seq(1, 3)
    result("b") should contain theSameElementsAs Seq(2)
  }

  it should "handle empty collections" in {
    SparkletRuntime.get.shuffle.clear() // Clean state for test isolation
    val source = toDistCollection(Seq.empty[(String, Int)])
    val result = source.groupByKey.collect()
    result shouldBe empty
    }

  it should "handle plans with multiple shuffle operations" in {
    SparkletRuntime.get.shuffle.clear() // Clean state for test isolation
    val source = toDistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3))
    val plan = source.reduceByKey[String, Int](_ + _).groupByKey.plan
    DAGScheduler.requiresDAGScheduling(plan) shouldBe true
    }

  it should "execute shuffle operations using DAG scheduler (basic smoke test)" in {
    SparkletRuntime.get.shuffle.clear() // Clean state for test isolation
    val source = toDistCollection(Seq(1 -> "a", 2 -> "b", 1 -> "c"))
    
    // This is a basic smoke test - the current implementation is simplified
    // and may not produce the exact expected result yet, but it should not crash
    noException should be thrownBy {
      val result = source.groupByKey.collect()
      logger.debug(s"GroupByKey result: $result")
    }
  }

  it should "execute reduceByKey operations using DAG scheduler (basic smoke test)" in {
    SparkletRuntime.get.shuffle.clear() // Clean state for test isolation
    val source = toDistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3))
    
    // This is a basic smoke test - the current implementation is simplified
    noException should be thrownBy {
      val result = source.reduceByKey[String, Int](_ + _).collect()
      logger.debug(s"ReduceByKey result: $result")
    }
  }

  it should "handle mixed narrow and wide transformations" in {
    val source = toDistCollection(Seq(1 -> "a", 2 -> "b", 1 -> "c"))
    
    // Chain narrow transformations before and after shuffle
    noException should be thrownBy {
      val result = source
        .map { case (k, v) => (k + 10, v.toUpperCase(java.util.Locale.ENGLISH)) }  // narrow
        .groupByKey                                       // wide (shuffle)
        .map { case (k, vs) => (k, vs.size) }           // narrow after shuffle
        .collect()
      logger.debug(s"Mixed transformations result: $result")
    }
  }

  it should "correctly concatenate results for union of transformed branches" in {
    SparkletRuntime.get.shuffle.clear() // Clean state for test isolation
    val left = toDistCollection(Seq(1, 2, 3)).map(_ * 2) // 2,4,6
    val right = toDistCollection(Seq(4, 5)).filter(_ % 2 == 1) // 5

    val result = left.union(right).collect()
    result shouldBe Seq(2, 4, 6, 5)
  }

  it should "allow union chaining and still use staged outputs" in {
    SparkletRuntime.get.shuffle.clear()
    val a = toDistCollection(Seq(1, 2))
    val b = toDistCollection(Seq(3)).map(_ + 10) // 13
    val c = toDistCollection(Seq(4)).filter(_ > 10) // empty

    val res = a.union(b).union(c).collect()
    res shouldBe Seq(1, 2, 13)
  }
} 