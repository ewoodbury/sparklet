package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection

class TestActionContracts extends AnyFlatSpec with Matchers {

  "take" should "return empty for take(0)" in {
    val result = DistCollection(Seq(1, 2, 3), 2).take(0)

    result shouldEqual List.empty[Int]
  }

  it should "return empty for take(0) on an empty collection" in {
    val result = DistCollection(Seq.empty[Int], 2).take(0)

    result shouldEqual List.empty[Int]
  }

  "first" should "return the first element" in {
    DistCollection(Seq(7, 8, 9), 3).first() shouldEqual 7
  }

  it should "throw on an empty collection" in {
    an[NoSuchElementException] should be thrownBy DistCollection(Seq.empty[Int], 2).first()
  }

  "reduce" should "throw on empty input" in {
    an[NoSuchElementException] should be thrownBy DistCollection(Seq.empty[Int], 2).reduce(_ + _)
  }

  "fold" should "return the initial value for empty input" in {
    DistCollection(Seq.empty[Int], 3).fold(10)(_ + _) shouldEqual 10
  }

  "aggregate" should "apply seqOp per partition and combOp across partitions" in {
    val dc = DistCollection(Seq(1, 2, 3, 4), 2)

    val result = dc.aggregate(List.empty[String])(
      seqOp = (acc, element) => acc :+ s"s$element",
      combOp = (left, right) => left ++ Seq("C") ++ right,
    )

    result shouldEqual Seq("s1", "s2", "C", "s3", "s4")
  }

  it should "return zero without invoking either function for empty input" in {
    val result = DistCollection(Seq.empty[Int], 2).aggregate("zero")(
      seqOp = (acc, element) => acc + element.toString,
      combOp = (left, right) => s"$left+$right",
    )

    result shouldEqual "zero"
  }

  it should "aggregate correctly across many partitions" in {
    val result = DistCollection(1 to 100, 4).aggregate(0)(_ + _, _ + _)

    result shouldEqual 5050
  }

  it should "aggregate over a wide plan" in {
    val dc = DistCollection(Seq(("a", 1), ("b", 2), ("a", 3)), 2)

    val result = dc
      .groupByKey
      .flatMap { case (_, groupedValues) => groupedValues }
      .aggregate(0)(_ + _, _ + _)

    result shouldEqual 6
  }
}
