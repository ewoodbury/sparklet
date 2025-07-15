package com.ewoodbury.sparklet.localengine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestLocalKeyValueActions extends AnyFlatSpec with Matchers {
  val toDistCollection = [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))

  "LocalExecutor" should "execute a simple reduceByKey operation" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two", 3 -> "three"))
    val result = source.reduceByKey[Int, String]((a, b) => a + b)
    val expected = Map(1 -> "one", 2 -> "two", 3 -> "three")

    result shouldEqual expected
  }

  it should "execute a simple reduceByKey operation with multiple elements" in {
    val source = toDistCollection(Seq("one" -> 1, "one" -> 1, "two" -> 2, "two" -> 2))
    val result = source.reduceByKey[String, Int]((a, b) => a + b)
    val expected = Map("one" -> 2, "two" -> 4)

    result shouldEqual expected
  }

  it should "execute a simple groupByKey operation" in {
    val source = toDistCollection(Seq("one" -> 1, "one" -> 1, "two" -> 2, "two" -> 2))
    val result = source.groupByKey[String, Int]
    val expected = Map("one" -> Seq(1, 1), "two" -> Seq(2, 2))

    result shouldEqual expected
  }

  it should "execute a simple groupByKey operation with strings" in {
    val source = toDistCollection(Seq("one" -> "one", "one" -> "one", "two" -> "two", "two" -> "two"))
    val result = source.groupByKey[String, String]
    val expected = Map("one" -> Seq("one", "one"), "two" -> Seq("two", "two"))

    result shouldEqual expected
  }

  it should "execute a simple groupByKey with sum" in {
    val source = toDistCollection(Seq(1 -> 1, 1 -> 1, 2 -> 2, 2 -> 2))
    val result = source.groupByKey[Int, Int].mapValues(values => values.sum).toMap
    val expected = Map(1 -> 2, 2 -> 4)

    result shouldEqual expected
  }
}