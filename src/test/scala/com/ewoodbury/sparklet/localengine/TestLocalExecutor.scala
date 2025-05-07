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

  it should "execute a simple flatMap operation" in {
    val source = DistCollection(Seq(1, 2, 3, 4))
    val flatMapped = source.flatMap(x => Seq(x, x * 2))
    val result = flatMapped.collect()
    val expected = Seq(1, 2, 2, 4, 3, 6, 4, 8)

    result shouldEqual expected
  }

  it should "execute a simple distinct operation" in {
    val source = DistCollection(Seq(1, 2, 2, 3, 4, 4))
    val distinct = source.distinct()
    val result = distinct.collect()
    val expected = Seq(1, 2, 3, 4)

    result shouldEqual expected
  }

  it should "execute a simple union operation" in {
    val left = DistCollection(Seq(1, 2, 3))
    val right = DistCollection(Seq(4, 5, 6))
    val union = left.union(right)

    val result = union.collect()
    val expected = Seq(1, 2, 3, 4, 5, 6)

    result shouldEqual expected
  }

  it should "execute a simple union operation with strings" in {
    val left = DistCollection(Seq("a", "b", "c" ))
    val right = DistCollection(Seq("d", "e", "f"))
    val union = left.union(right)
    val result = union.collect()
    val expected = Seq("a", "b", "c", "d", "e", "f")

    result shouldEqual expected
  }

  it should "execute a simple union operation with other transformations" in {
    val source1 = DistCollection(Seq(1, 1))
    val transform1 = source1.map(_ * 2)
    
    val source2 = DistCollection(Seq(3, 4))
    val transform2 = source2.filter(_ % 2 == 0)

    val union = transform1.union(transform2)
    val result = union.collect()
    val expected = Seq(2, 2, 4)

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

  it should "execute a simple take operation" in {
    val source = DistCollection(Seq(1, 2, 3, 4, 5))
    val taken = source.take(3)
    val result = taken.collect()
    val expected = Seq(1, 2, 3)

    result shouldEqual expected
  }
  
}
