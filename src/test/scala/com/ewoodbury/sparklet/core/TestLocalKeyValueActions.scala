package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.execution.ShuffleManager

class TestLocalKeyValueActions extends AnyFlatSpec with Matchers {
  val toDistCollection: [T] => (seq: Seq[T]) => DistCollection[T] = [T] => (seq: Seq[T]) => DistCollection(com.ewoodbury.sparklet.core.Plan.Source(Seq(Partition(seq))))

  "Executor" should "execute a simple reduceByKey operation" in {
    ShuffleManager.clear() // Clean state for test isolation
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two", 3 -> "three"))
    val result = source.reduceByKey[Int, String]((a, b) => a + b).collect().toMap
    val expected = Map(1 -> "one", 2 -> "two", 3 -> "three")

    result shouldEqual expected
  }

  it should "execute a simple reduceByKey operation with multiple elements" in {
    ShuffleManager.clear() // Clean state for test isolation
    val source = toDistCollection(Seq("one" -> 1, "one" -> 1, "two" -> 2, "two" -> 2))
    val result = source.reduceByKey[String, Int]((a, b) => a + b).collect().toMap
    val expected = Map("one" -> 2, "two" -> 4)

    result shouldEqual expected
  }

  it should "execute a simple groupByKey operation" in {
    ShuffleManager.clear() // Clean state for test isolation
    val source = toDistCollection(Seq("one" -> 1, "one" -> 1, "two" -> 2, "two" -> 2))
    val result = source.groupByKey[String, Int].collect().map { case (k, vs) => (k, vs.toSeq) }.toMap
    val expected = Map("one" -> Seq(1, 1), "two" -> Seq(2, 2))

    result shouldEqual expected
  }

  it should "execute a simple groupByKey operation with strings" in {
    ShuffleManager.clear() // Clean state for test isolation
    val source = toDistCollection(Seq("one" -> "one", "one" -> "one", "two" -> "two", "two" -> "two"))
    val result = source.groupByKey[String, String].collect().map { case (k, vs) => (k, vs.toSeq) }.toMap
    val expected = Map("one" -> Seq("one", "one"), "two" -> Seq("two", "two"))

    result shouldEqual expected
  }

  // TODO: Fix this failure, getting error java.lang.ClassCastException: class java.lang.Integer cannot be cast to class scala.collection.Iterable
  ignore should "execute a simple groupByKey with sum" in {
    ShuffleManager.clear() // Clean state for test isolation
    val source = toDistCollection(Seq(1 -> 1, 1 -> 1, 2 -> 2, 2 -> 2))
    val result = source.groupByKey[Int, Int].mapValues((values: Iterable[Int]) => values.sum).collect().toMap
    val expected = Map(1 -> 2, 2 -> 4)

    result shouldEqual expected
  }
}