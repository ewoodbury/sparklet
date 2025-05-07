package com.ewoodbury.sparklet.localengine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestLocalActions extends AnyFlatSpec with Matchers {
  "LocalExecutor" should "execute a simple collect operation" in {
    val source = DistCollection(Seq(1, 2, 3, 4, 5))
    val result = source.collect()
    val expected = Seq(1, 2, 3, 4, 5)

    result shouldEqual expected
  }

  it should "execute a simple take operation" in {
    val source = DistCollection(Seq(1, 2, 3, 4, 5))
    val taken = source.take(3)
    val result = taken.collect()
    val expected = Seq(1, 2, 3)

    result shouldEqual expected
  }

  it should "execute a simple first operation" in {
    val source = DistCollection(Seq(1, 2, 3, 4, 5))
    val first = source.first()
    val result = first
    val expected = 1

    result shouldEqual expected
  }

  it should "execute a simple reduce operation" in {
    val source = DistCollection(Seq(1, 2, 3, 4, 5))
    val reduced = source.reduce(_ + _)
    val result = reduced
    val expected = 15

    result shouldEqual expected
  }

  it should "execute a simple reduce operation with strings" in {
    val source = DistCollection(Seq("a", "b", "c"))
    val reduced = source.reduce(_ + _)
    val result = reduced
    val expected = "abc"

    result shouldEqual expected
  }

  it should "execute a simple fold operation" in {
    val source = DistCollection(Seq(1, 2, 3, 4, 5))
    val folded = source.fold(0)(_ + _)
    val result = folded
    val expected = 15

    result shouldEqual expected
  }
}
