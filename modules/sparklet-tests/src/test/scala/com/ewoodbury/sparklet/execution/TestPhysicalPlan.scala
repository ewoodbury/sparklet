package com.ewoodbury.sparklet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.core.SparkletConf
import com.ewoodbury.sparklet.runtime.SparkletRuntime

/**
 * The columnar path lowers one physical node per operator and runs that tree. Fusion is not this
 * step.
 */
class TestPhysicalPlan extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = SparkletConf.get

  override def afterEach(): Unit =
    SparkletRuntime.get.shuffle.clear()
    SparkletConf.set(originalConf)
    ()

  "an int pipeline" should "keep one project per map" in {
    val calls = new AtomicInteger
    val mapped = DistCollection(Seq(1, 2, 3), 1)
      .map { value =>
        calls.incrementAndGet()
        value + 1
      }
      .map(_ + 1)
      .map(_ + 1)
      .map(_ + 1)
    shape(mapped) shouldBe "Project(Project(Project(Project(Scan))))"
    calls.get() shouldBe 0
    mapped.collect() shouldBe Seq(5, 6, 7)
    calls.get() shouldBe 3
  }

  it should "keep filter and project as separate nodes" in {
    val pipeline = DistCollection(Seq(1, 2, 3, 4), 2).filter(_ % 2 == 0).map(_ * 10)
    shape(pipeline) shouldBe "Project(Filter(Scan))"
    pipeline.collect() shouldBe Seq(20, 40)
  }

  "a pair plan" should "stop a filter at the aggregate" in {
    val reduced = DistCollection(Seq(1 -> 1, 1 -> 5, 2 -> 0), 1)
      .filterValues[Int, Int](_ > 0)
      .reduceByKey[Int, Int](_ + _)
    shape(reduced) shouldBe "HashAggregate(Exchange(FilterValue(Scan)))"
    reduced.collect().toMap shouldBe Map(1 -> 6)
  }

  it should "name a key filter and a row filter" in {
    val keys = DistCollection(Seq(1 -> 1, 2 -> 2), 1).filterKeys[Int, Int](_ == 1)
    shape(keys) shouldBe "FilterKey(Scan)"
    val rows = DistCollection(Seq(1 -> 1, 2 -> 2), 1).filter((pair: (Int, Int)) => pair._1 == 1)
    shape(rows) shouldBe "FilterRow(Scan)"
  }

  it should "exchange each join input, including an aggregate input" in {
    val build = DistCollection(Seq(1 -> 1, 1 -> 2), 1).reduceByKey[Int, Int](_ + _)
    val probe = DistCollection(Seq(1 -> 10), 1)
    shape(build.join(probe)) shouldBe
      "HashJoin(Exchange(HashAggregate(Exchange(Scan))), Exchange(Scan))"
  }

  it should "leave a pair map on the row path" in {
    val mapped = DistCollection(Seq(1 -> 1, 1 -> 2), 1).map { (key, value) =>
      (key, value + 1)
    }
    ColumnarPlanner.lower(mapped.plan) shouldBe None
    val reduced = mapped.reduceByKey[Int, Int](_ + _)
    ColumnarPlanner.lower(reduced.plan) shouldBe None
    reduced.collect().toMap shouldBe Map(1 -> 5)
  }

  it should "leave a map into a pair on the row path" in {
    val counted = DistCollection(Seq(1, 1, 2), 1)
      .map(value => (value, 1))
      .reduceByKey[Int, Int](_ + _)
    ColumnarPlanner.lower(counted.plan) shouldBe None
    counted.collect().toMap shouldBe Map(1 -> 2, 2 -> 1)
  }

  "lowering" should "be empty when columnar execution is off" in {
    SparkletConf.set(SparkletConf.get.copy(columnarExecution = false))
    val mapped = DistCollection(Seq(1, 2), 1).map(_ + 1)
    ColumnarPlanner.lower(mapped.plan) shouldBe None
    mapped.collect() shouldBe Seq(2, 3)
  }

  private def shape[A](collection: DistCollection[A]): String =
    ColumnarPlanner
      .lower(collection.plan)
      .map(_.shape)
      .getOrElse(fail("plan stayed on the row path"))
