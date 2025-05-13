package com.ewoodbury.sparklet.localengine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestLocalKeyValueActions extends AnyFlatSpec with Matchers {
  "LocalExecutor" should "execute a simple reduceByKey operation" in {
    val source = DistCollection(Seq(1 -> "one", 2 -> "two", 3 -> "three"))
    val result = source.reduceByKey[Int, String]((a, b) => a + b)
    val expected = Map(1 -> "one", 2 -> "two", 3 -> "three")

    result shouldEqual expected
  }

  it should "execute a simple reduceByKey operation with multiple elements" in {
    val source = DistCollection(Seq("one" -> 1, "one" -> 1, "two" -> 2, "two" -> 2))
    val result = source.reduceByKey[String, Int]((a, b) => a + b)
    val expected = Map("one" -> 2, "two" -> 4)

    result shouldEqual expected
  }
}