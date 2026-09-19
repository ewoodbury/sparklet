package com.ewoodbury.sparklet.execution

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.core.{Partition, Plan}
import com.ewoodbury.sparklet.runtime.SparkletRuntime

class TestDAGScheduler extends AnyFlatSpec with Matchers with StrictLogging {
  val toDistCollection: [T] => (seq: Seq[T]) => DistCollection[T] = [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))

  // Unified execution path: every plan (narrow and wide) compiles to a stage graph and runs
  // through the same DAGScheduler entry point.

  it should "execute simple narrow transformations" in {
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

  it should "support mapPartitions" in {
    SparkletRuntime.get.shuffle.clear()
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val result = source.mapPartitions(it => Iterator.single(it.sum)).collect()
    result shouldBe Seq(15)
  }

  it should "support repartition" in {
    SparkletRuntime.get.shuffle.clear()
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val result = source.repartition(3).collect()
    result.toSet shouldBe Set(1, 2, 3, 4, 5)
  }

  it should "support coalesce" in {
    SparkletRuntime.get.shuffle.clear()
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val result = source.coalesce(3).collect()
    result.toSet shouldBe Set(1, 2, 3, 4, 5)
  }

  it should "support partitionBy for key-value datasets" in {
    SparkletRuntime.get.shuffle.clear()
    val source = toDistCollection(Seq("a" -> 1, "b" -> 2, "c" -> 3, "a" -> 4))
    val result = source.partitionBy[String, Int](numPartitions = 3).reduceByKey[String, Int](_ + _).collect().toMap
    result("a") shouldBe 5
    result("b") shouldBe 2
    result("c") shouldBe 3
  }

  it should "support mapPartitions, repartition, and coalesce together" in {
    SparkletRuntime.get.shuffle.clear()
    val data = (1 to 30).toList
    val ds = toDistCollection(data)
      .mapPartitions(it => Iterator.single(it.sum)) // 1 sum per source partition
      .repartition(7)
      .coalesce(3)
    val result = ds.collect().toList
    result.sum shouldBe data.sum
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
    val result = source.reduceByKey[String, Int](_ + _).groupByKey.collect().toMap
    result("a") should contain theSameElementsAs Seq(4)
    result("b") should contain theSameElementsAs Seq(2)
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

  it should "execute source-only, narrow, and wide plans through the same scheduler path" in {
    SparkletRuntime.get.shuffle.clear()
    val rt = SparkletRuntime.get
    val scheduler = new DAGScheduler[IO](rt.shuffle, rt.scheduler, rt.partitioner)

    // Source-only plan
    val sourcePlan = Plan.Source(Seq(Partition(Seq(1, 2)), Partition(Seq(3))))
    scheduler.executePartitions(sourcePlan).unsafeRunSync().flatMap(_.data).toSeq shouldBe Seq(1, 2, 3)

    // Narrow plan: one-stage graph
    val narrowPlan = Plan.MapOp(
      Plan.FilterOp(sourcePlan, (x: Int) => x > 1),
      (x: Int) => x * 10,
    )
    scheduler.execute(narrowPlan).unsafeRunSync().toSeq shouldBe Seq(20, 30)

    // Wide plan: multi-stage graph
    val widePlan = Plan.GroupByKeyOp[Int, String](
      Plan.Source(Seq(Partition(Seq(1 -> "x")), Partition(Seq(1 -> "y")))),
    )
    val wideResult = scheduler.execute(widePlan).unsafeRunSync().toMap
    wideResult(1) should contain theSameElementsAs Seq("x", "y")
  }
}

