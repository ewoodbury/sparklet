package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.execution.ShuffleManager

class TestJoins extends AnyFlatSpec with Matchers {

  private val toDistCollection: [T] => (seq: Seq[T]) => DistCollection[T] =
    [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))

  it should "read the explicit left/right join inputs, not unrelated prior shuffles" in {
    // This test is for a specific bug where joins were using recent shuffles as inputs,
    // rather than the explicit left/right inputs.

    // Arrange
    ShuffleManager.clear()

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
}



