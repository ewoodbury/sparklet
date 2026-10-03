package com.ewoodbury.scarlet.execution

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.BatchCodec
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/** The columnar path lowers physical trees and fuses short element filter/project runs. */
class TestPhysicalPlan extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
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

  it should "call each function once per row" in {
    val calls = Array(0, 0, 0, 0)
    val mapped = DistCollection(Seq(1, 2, 3), 1)
      .map { value =>
        calls(0) += 1
        value + 1
      }
      .map { value =>
        calls(1) += 1
        value + 1
      }
      .filter { value =>
        calls(2) += 1
        value > 0
      }
      .map { value =>
        calls(3) += 1
        value + 1
      }
    shape(mapped) shouldBe "Pipeline4(Scan)"
    mapped.collect() shouldBe Seq(4, 5, 6)
    calls.toSeq shouldBe Seq(3, 3, 3, 3)
  }

  it should "skip the rest of the chain when a filter rejects the row" in {
    val calls = Array(0, 0, 0)
    val mapped = DistCollection(Seq(-1, 2, -3, 4), 1)
      .filter { value =>
        calls(0) += 1
        value > 0
      }
      .map { value =>
        calls(1) += 1
        value * 10
      }
      .filter { value =>
        calls(2) += 1
        value == 20
      }
    shape(mapped) shouldBe "Pipeline3(Scan)"
    mapped.collect() shouldBe Seq(20)
    calls.toSeq shouldBe Seq(4, 2, 2)
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
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    six.collect() shouldBe Seq(7, 8, 9)
  }

  it should "run later chunks on the values earlier chunks produced" in {
    val mapped = DistCollection(Seq(1), 1)
      .map(_ + 1)
      .map(_ * 10)
      .map(_ + 2)
      .map(_ * 10)
      .map(_ + 3)
      .map(_ * 10)
    shape(mapped) shouldBe "Pipeline2(Pipeline4(Scan))"
    mapped.collect() shouldBe Seq(2230)
  }

  it should "dispatch every two-, three-, and four-step pattern" in {
    assertPatterns(DistCollection(Seq(-2, -1, 0, 1, 2, 3), 1), _.filter(_ > 0), _.map(_ + 3))
  }

  "a float pipeline" should "fuse filter and project the way ints do" in {
    val one = DistCollection(Seq(-1.5, 0.0, 2.5), 1).map(_ + 1.0)
    shape(one) shouldBe "Project(Scan)"
    ColumnarPlanner.lower(one.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Floats)

    val pipeline = DistCollection(Seq(-1.5, 0.0, 2.5, 4.0), 2).filter(_ > 0.0).map(_ * 10.0)
    shape(pipeline) shouldBe "Pipeline2(Scan)"
    pipeline.collect() shouldBe Seq(25.0, 40.0)

    val five = DistCollection(Seq(1.0, 2.0), 1)
      .map(_ + 1.0)
      .map(_ + 1.0)
      .map(_ + 1.0)
      .map(_ + 1.0)
      .map(_ + 1.0)
    shape(five) shouldBe "Project(Pipeline4(Scan))"
    shape(five.map(_ + 1.0)) shouldBe "Pipeline2(Pipeline4(Scan))"
  }

  it should "call each function once per row" in {
    val calls = Array(0, 0, 0, 0)
    val mapped = DistCollection(Seq(1.0, 2.0), 1)
      .map { value =>
        calls(0) += 1
        value + 1.0
      }
      .filter { value =>
        calls(1) += 1
        value > 0.0
      }
      .map { value =>
        calls(2) += 1
        value + 1.0
      }
      .map { value =>
        calls(3) += 1
        value + 1.0
      }
    shape(mapped) shouldBe "Pipeline4(Scan)"
    mapped.collect() shouldBe Seq(4.0, 5.0)
    calls.toSeq shouldBe Seq(2, 2, 2, 2)
  }

  it should "skip the rest of the chain when a filter rejects the row" in {
    val calls = Array(0, 0, 0)
    val mapped = DistCollection(Seq(-1.0, 2.0, -3.0, 4.0), 1)
      .filter { value =>
        calls(0) += 1
        value > 0.0
      }
      .map { value =>
        calls(1) += 1
        value * 10.0
      }
      .filter { value =>
        calls(2) += 1
        value == 20.0
      }
    shape(mapped) shouldBe "Pipeline3(Scan)"
    mapped.collect() shouldBe Seq(20.0)
    calls.toSeq shouldBe Seq(4, 2, 2)
  }

  it should "run later chunks on the values earlier chunks produced" in {
    val mapped = DistCollection(Seq(1.0), 1)
      .map(_ + 1.0)
      .map(_ * 10.0)
      .map(_ + 2.0)
      .map(_ * 10.0)
      .map(_ + 3.0)
      .map(_ * 10.0)
    shape(mapped) shouldBe "Pipeline2(Pipeline4(Scan))"
    mapped.collect() shouldBe Seq(2230.0)
  }

  it should "dispatch every two-, three-, and four-step pattern" in {
    assertPatterns(
      DistCollection(Seq(-1.5, 0.0, 2.5), 1),
      _.filter(_ > 0.0),
      _.map(_ + 3.0),
    )
  }

  it should "leave a 32-bit float on the row path" in {
    val mapped = DistCollection(Seq(1.0f, 2.0f), 1).map(_ + 1.0f).map(_ + 1.0f)
    ColumnarPlanner.lower(mapped.plan) shouldBe rowPath
    mapped.collect() shouldBe Seq(3.0f, 4.0f)
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

  "a string pipeline" should "fuse filter and project" in {
    val mapped = DistCollection(Seq("aa", "b", "cc"), 1)
      .filter(_.length == 1)
      .map(_.toUpperCase(Locale.ENGLISH))
    shape(mapped) shouldBe "Pipeline2(Scan)"
    mapped.collect() shouldBe Seq("B")
    shape(DistCollection(Seq("a", "bb"), 1).filter(_.nonEmpty)) shouldBe "Filter(Scan)"
    shape(DistCollection(Seq("a"), 1).map(_.toUpperCase(Locale.ENGLISH))) shouldBe "Project(Scan)"
  }

  it should "skip the rest of the chain when a filter rejects the row" in {
    val calls = Array(0, 0, 0)
    val mapped = DistCollection(Seq("a", "bb", "c", "dd"), 1)
      .filter { value =>
        calls(0) += 1
        value.length > 1
      }
      .map { value =>
        calls(1) += 1
        value + "!"
      }
      .filter { value =>
        calls(2) += 1
        value.startsWith("b")
      }
    shape(mapped) shouldBe "Pipeline3(Scan)"
    mapped.collect() shouldBe Seq("bb!")
    calls.toSeq shouldBe Seq(4, 2, 2)
  }

  it should "keep a null the predicate accepts" in {
    val rows = Seq(Some("aa"), None, Some("b")).map(BatchCodec.stringElement)
    val mapped = DistCollection(rows, 1)
      .filter(text => Option(text).isEmpty || text.length > 1)
      .map { text =>
        Option(text) match
          case Some(value) => value.toUpperCase(Locale.ENGLISH)
          case None => BatchCodec.stringElement(None)
      }
    shape(mapped) shouldBe "Pipeline2(Scan)"
    val columnar = mapped.collect()
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    columnar shouldBe mapped.collect()
  }

  it should "cut long runs into source-ordered groups of four" in {
    val five = DistCollection(Seq("a", "b"), 1)
      .map(_ + "1")
      .map(_ + "2")
      .map(_ + "3")
      .map(_ + "4")
      .map(_ + "5")
    val six = five.map(_ + "6")
    shape(five) shouldBe "Project(Pipeline4(Scan))"
    shape(six) shouldBe "Pipeline2(Pipeline4(Scan))"
    five.collect() shouldBe Seq("a12345", "b12345")
    six.collect() shouldBe Seq("a123456", "b123456")
  }

  it should "dispatch every two-, three-, and four-step pattern" in {
    assertPatterns(
      DistCollection(Seq("a", "bb", "ccc"), 1),
      _.filter(_.length > 1),
      _.map(_ + "x"),
    )
  }

  "lowering" should "be empty when columnar execution is off" in {
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
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

  private def assertPatterns[A](
      seed: DistCollection[A],
      filterStep: DistCollection[A] => DistCollection[A],
      mapStep: DistCollection[A] => DistCollection[A],
  ): Unit =
    val pipelines = for
      length <- 2 to 4
      pattern <- 0 until (1 << length)
    yield
      val plan = (0 until length).foldLeft(seed) { (current, index) =>
        if (((pattern >>> index) & 1) == 0) then filterStep(current) else mapStep(current)
      }
      (length, plan)

    pipelines.length shouldBe 28
    pipelines.foreach { (length, plan) =>
      shape(plan) shouldBe s"Pipeline$length(Scan)"
    }
    val columnar = pipelines.map((_, plan) => plan.collect())
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    columnar shouldBe pipelines.map((_, plan) => plan.collect())
