package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.runtime.api.SparkletRuntime

class TestJoins extends AnyFlatSpec with Matchers {

  private val toDistCollection: [T] => (seq: Seq[T]) => com.ewoodbury.sparklet.api.DistCollection[T] =
    [T] => (seq: Seq[T]) => com.ewoodbury.sparklet.api.DistCollection(Plan.Source(Seq(Partition(seq))))

  it should "read the explicit left/right join inputs, not unrelated prior shuffles" in {
    // This test is for a specific bug where joins were using recent shuffles as inputs,
    // rather than the explicit left/right inputs.

    // Arrange
    SparkletRuntime.get.shuffle.clear()

    val left  = toDistCollection(Seq("a" -> 1))
    val right = toDistCollection(Seq("a" -> 2))

    // Create an unrelated branch that performs a shuffle before the join executes
    val unrelatedShuffled: DistCollection[(String, (Int, Int))] =
      toDistCollection(Seq("u" -> 999))
        .groupByKey
        .map { case (k, vs) => (k, (vs.headOption.getOrElse(0), -1)) }

    // Build a plan where the unrelated branch is on the left of a union, so its shuffle stages
    // are constructed and (by current build order) scheduled prior to the join stage.
    val combined: DistCollection[(String, (Int, Int))] =
      unrelatedShuffled.union(left.join(right))

    // Act
    val allResults = combined.collect().toSeq

    // Assert: The join result for key "a" should be present. With the current heuristic that
    // scans prior shuffles, the join may incorrectly read the unrelated shuffle as one side and
    // thus drop the "a" result, making this assertion fail.
    val joinOnly = allResults.filter(_._1 === "a")
    joinOnly shouldBe Seq("a" -> (1, 2))
  }

  it should "perform inner join as cartesian product for matching keys" in {
    // Arrange
    SparkletRuntime.get.shuffle.clear()

    val left  = toDistCollection(Seq("a" -> 1, "a" -> 3, "b" -> 7))
    val right = toDistCollection(Seq("a" -> 2, "a" -> 4, "c" -> 9))

    // Act
    val results = left.join(right).collect().toSeq

    // Assert
    val expectedForA = Seq(
      "a" -> (1, 2),
      "a" -> (1, 4),
      "a" -> (3, 2),
      "a" -> (3, 4),
    )

    val forA = results.filter(_._1 === "a")
    forA should contain theSameElementsAs expectedForA

    // Only keys present in both sides should appear (inner join semantics)
    results.map(_._1).toSet shouldBe Set("a")
  }

  it should "handle multi-key joins with cartesian product per key" in {
    // Arrange
    SparkletRuntime.get.shuffle.clear()

    val left  = toDistCollection(Seq("a" -> 1, "a" -> 3, "b" -> 5))
    val right = toDistCollection(Seq("a" -> 2, "a" -> 4, "b" -> 6, "b" -> 7))

    // Act
    val results = left.join(right).collect().toSeq

    // Assert per key
    val expectedA = Seq(
      "a" -> (1, 2),
      "a" -> (1, 4),
      "a" -> (3, 2),
      "a" -> (3, 4),
    )
    val expectedB = Seq(
      "b" -> (5, 6),
      "b" -> (5, 7),
    )

    val forA = results.filter(_._1 === "a")
    val forB = results.filter(_._1 === "b")

    forA should contain theSameElementsAs expectedA
    forB should contain theSameElementsAs expectedB

    // Only keys present in both sides should appear
    results.map(_._1).toSet shouldBe Set("a", "b")
  }

  it should "return empty results when either side is empty" in {
    // Arrange
    SparkletRuntime.get.shuffle.clear()

    val nonEmpty = toDistCollection(Seq("x" -> 1, "y" -> 2))
    val emptyDC  = toDistCollection(Seq.empty[(String, Int)])

    // Act + Assert
    nonEmpty.join(emptyDC).collect().toSeq shouldBe empty
    emptyDC.join(nonEmpty).collect().toSeq shouldBe empty
  }
}


