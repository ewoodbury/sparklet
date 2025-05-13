package com.ewoodbury.sparklet.localengine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestLocalKeyValueTransformations extends AnyFlatSpec with Matchers {
  "LocalExecutor" should "execute a simple mapValues operation" in {
    val source = DistCollection(Seq(1 -> "one", 2 -> "two", 3 -> "three"))
    val result = source.mapValues[Int, String, String](_ + "!").collect()
    val expected = Seq(1 -> "one!", 2 -> "two!", 3 -> "three!")

    result shouldEqual expected
  }

  it should "execute a simple filterKeys operation" in {
    val source = DistCollection(Seq(1 -> "one", 2 -> "two", 3 -> "three"))
    val result = source.filterKeys[Int, String](_ % 2 == 0).collect()
    val expected = Seq(2 -> "two")

    result shouldEqual expected
  }
}