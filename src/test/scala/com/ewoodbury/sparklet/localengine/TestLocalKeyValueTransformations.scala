package com.ewoodbury.sparklet.localengine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestLocalKeyValueTransformations extends AnyFlatSpec with Matchers {
  val toDistCollection = [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))
  
  "LocalExecutor" should "execute a simple keys operation" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val keys = source.keys
    val result = keys.collect()
    val expected = Seq(1, 2)

    result shouldEqual expected
  }

  it should "execute a simple values operation" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val values = source.values
    val result = values.collect()
    val expected = Seq("one", "two")

    result shouldEqual expected
  }

  it should "execute a simple mapValues operation" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val result = source.mapValues[Int, String, String](_ + "!").collect()
    val expected = Seq(1 -> "one!", 2 -> "two!")

    result shouldEqual expected
  }

  it should "execute a simple filterKeys operation" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val result = source.filterKeys[Int, String](_ % 2 == 0).collect()
    val expected = Seq(2 -> "two")

    result shouldEqual expected
  }

  it should "execute a simple flatMapValues operation" in {
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two"))
    val result = source.flatMapValues[Int, String, String](_.split("")).collect()
    val expected = Seq(1 -> "o", 1 -> "n", 1 -> "e", 2 -> "t", 2 -> "w", 2 -> "o")

    result shouldEqual expected
  }
}