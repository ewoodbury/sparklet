package com.ewoodbury.sparklet.execution

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.core.SparkletConf
import com.ewoodbury.sparklet.runtime.SparkletRuntime

/**
 * The columnar path lowers physical trees and fuses short int filter/project runs.
 */
class TestPhysicalPlan extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = SparkletConf.get

  override def afterEach(): Unit =
    SparkletRuntime.get.shuffle.clear()
    SparkletConf.set(originalConf)
    ()

  "an int pipeline" should "fuse four maps into one project loop" in {
    shape(DistCollection(Seq(1, 2, 3), 1).map(_ + 1)) shouldBe "Project(Scan)"
    val calls = new AtomicInteger
    val mapped = DistCollection(Seq(1, 2, 3), 1)
      .map { value =>
        calls.incrementAndGet()
        value + 1
      }
      .map(_ + 1)
      .map(_ + 1)
      .map(_ + 1)
    shape(mapped) shouldBe "Pipeline4(Scan)"
    calls.get() shouldBe 0
    mapped.collect() shouldBe Seq(5, 6, 7)
    calls.get() shouldBe 3
  }

  it should "fuse a filter and project" in {
    val pipeline = DistCollection(Seq(1, 2, 3, 4), 2).filter(_ % 2 == 0).map(_ * 10)
    shape(pipeline) shouldBe "Pipeline2(Scan)"
    pipeline.collect() shouldBe Seq(20, 40)
  }

  it should "cut long runs into source-ordered groups of four" in {
    val five = DistCollection(Seq(1, 2, 3), 1)
      .map(_ + 1)
      .map(_ + 1)
      .map(_ + 1)
      .map(_ + 1)
      .map(_ + 1)
    val six = five.map(_ + 1)

    shape(five) shouldBe "Project(Pipeline4(Scan))"
    shape(six) shouldBe "Pipeline2(Pipeline4(Scan))"
    six.collect() shouldBe Seq(7, 8, 9)
    SparkletConf.set(SparkletConf.get.copy(columnarExecution = false))
    six.collect() shouldBe Seq(7, 8, 9)
  }

  it should "dispatch every two-, three-, and four-step pattern" in {
    val pipelines = for
      length <- 2 to 4
      pattern <- 0 until (1 << length)
    yield
      (0 until length).foldLeft(DistCollection(Seq(-2, -1, 0, 1, 2, 3), 1)) {
        (current, index) =>
          if (((pattern >>> index) & 1) == 0) then current.filter(_ > 0)
          else current.map(_ + 3)
      }

    pipelines.foreach { pipeline =>
      shape(pipeline) should startWith("Pipeline")
    }
    val columnar = pipelines.map(_.collect())
    SparkletConf.set(SparkletConf.get.copy(columnarExecution = false))
    columnar shouldBe pipelines.map(_.collect())
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
    val joined = build.join(probe)
    shape(joined) shouldBe
      "HashJoin(Exchange(HashAggregate(Exchange(Scan))), Exchange(Scan))"
    joined.collect().toMap shouldBe Map(1 -> ((3, 10)))
  }

  it should "leave a pair map on the row path" in {
    val mapped = DistCollection(Seq(1 -> 1, 1 -> 2), 1).map { (key, value) =>
      (key, value + 1)
    }
    ColumnarPlanner.lower(mapped.plan) shouldBe rowPath
    val reduced = mapped.reduceByKey[Int, Int](_ + _)
    ColumnarPlanner.lower(reduced.plan) shouldBe rowPath
    reduced.collect().toMap shouldBe Map(1 -> 5)
  }

  it should "leave a map into a pair on the row path" in {
    val counted = DistCollection(Seq(1, 1, 2), 1)
      .map(value => (value, 1))
      .reduceByKey[Int, Int](_ + _)
    ColumnarPlanner.lower(counted.plan) shouldBe rowPath
    counted.collect().toMap shouldBe Map(1 -> 2, 2 -> 1)
  }

  "a string pipeline" should "keep filter and project as separate nodes" in {
    val mapped = DistCollection(Seq("aa", "b", "cc"), 1)
      .filter(_.length == 1)
      .map(_.toUpperCase(Locale.ENGLISH))
    shape(mapped) shouldBe "Project(Filter(Scan))"
    mapped.collect() shouldBe Seq("B")
  }

  "lowering" should "be empty when columnar execution is off" in {
    SparkletConf.set(SparkletConf.get.copy(columnarExecution = false))
    val mapped = DistCollection(Seq(1, 2), 1).map(_ + 1)
    ColumnarPlanner.lower(mapped.plan) shouldBe rowPath
    mapped.collect() shouldBe Seq(2, 3)
  }

  private val rowPath = Option.empty[PhysicalOp]

  private def shape[A](collection: DistCollection[A]): String =
    ColumnarPlanner
      .lower(collection.plan)
      .map(_.shape)
      .getOrElse(fail("plan stayed on the row path"))
