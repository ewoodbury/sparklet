package com.ewoodbury.sparklet.localengine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestLocalExecutor extends AnyFlatSpec with Matchers {
  "LocalExecutor" should "execute a simple map operation" in {
    val source = DistCollection(Seq(1, 2, 3, 4, 5))
    val mapped = source.map(_ * 2)
    val result = mapped.collect()
    val expected = Seq(2, 4, 6, 8, 10)

    result shouldEqual expected
  }

  it should "execute a simple filter operation" in {
    val source = DistCollection(Seq(1, 2, 3, 4, 5))
    val filtered = source.filter(_ % 2 == 0)
    val result = filtered.collect()
    val expected = Seq(2, 4)

    result shouldEqual expected
  }

  it should "execute a simple map and filter operation" in {
    val source = DistCollection(Seq(1, 2, 3, 4, 5))
    val mapped = source.map(_ * 2)
    val filtered = mapped.filter(_ % 4 == 0)
    val result = filtered.collect()
    val expected = Seq(4, 8)

    result shouldEqual expected
  }
}
