package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection

class TestLocalTransformations extends AnyFlatSpec with Matchers {
  val toDistCollection: [T] => (seq: Seq[T]) => DistCollection[T] = [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))
  val sourceInt: DistCollection[Int] = toDistCollection(Seq(1, 2, 3, 4, 5))

  "Executor" should "execute a simple map operation" in {
    val mapped = sourceInt.map(_ * 2)
    val result = mapped.collect()
    val expected = Seq(2, 4, 6, 8, 10)

    result shouldEqual expected
  }

  it should "execute a simple filter operation" in {
    val filtered = sourceInt.filter(_ % 2 == 0)
    val result = filtered.collect()
    val expected = Seq(2, 4)

    result shouldEqual expected
  }

  it should "execute a simple map and filter operation" in {
    val mapped = sourceInt.map(_ * 2)
    val filtered = mapped.filter(_ % 4 == 0)
    val result = filtered.collect()
    val expected = Seq(4, 8)

    result shouldEqual expected
  }

  it should "execute a simple flatMap operation" in {
    val flatMapped = sourceInt.flatMap(x => Seq(x, x * 2))
    val result = flatMapped.collect()
    val expected = Seq(1, 2, 2, 4, 3, 6, 4, 8, 5, 10)

    result shouldEqual expected
  }

  it should "execute a simple distinct operation" in {
    val source = toDistCollection(Seq(1, 2, 2, 3, 4, 4))
    val distinct = source.distinct()
    val result = distinct.collect()
    val expected = Seq(1, 2, 3, 4)

    result shouldEqual expected
  }

  it should "execute a simple union operation with ints" in {
    val left = toDistCollection(Seq(1, 2, 3))
    val right = toDistCollection(Seq(4, 5, 6))
    val union = left.union(right)

    val result = union.collect()
    val expected = Seq(1, 2, 3, 4, 5, 6)

    result shouldEqual expected
  }

  it should "execute a simple union operation with strings" in {
    val left = toDistCollection(Seq("a", "b", "c"))
    val right = toDistCollection(Seq("d", "e", "f"))
    val union = left.union(right)
    val result = union.collect()
    val expected = Seq("a", "b", "c", "d", "e", "f")

    result shouldEqual expected
  }

  it should "execute a simple union operation with other transformations" in {
    val source1 = toDistCollection(Seq(1, 1))
    val transform1 = source1.map(_ * 2)
    
    val source2 = toDistCollection(Seq(3, 4))
    val transform2 = source2.filter(_ % 2 == 0)

    val union = transform1.union(transform2)
    val result = union.collect()
    val expected = Seq(2, 2, 4)

    result shouldEqual expected
  }

  it should "union two branches after independent transforms (correct concatenation)" in {
    val left = toDistCollection(Seq(1, 2, 3)).map(_ * 10) // 10,20,30
    val right = toDistCollection(Seq(4, 5)).filter(_ % 2 == 1).map(_ + 100) // 5 -> 105

    val union = left.union(right)
    val result = union.collect()

    result shouldEqual Seq(10, 20, 30, 105)
  }

  it should "preserve prior narrow ops across multiple unions" in {
    val a = toDistCollection(Seq(1, 2)).map(_ + 1) // 2,3
    val b = toDistCollection(Seq(3)).map(_ * 2)   // 6
    val c = toDistCollection(Seq(7)).filter(_ > 10) // empty

    val u1 = a.union(b) // 2,3,6
    val u2 = u1.union(c) // 2,3,6

    u2.collect() shouldEqual Seq(2, 3, 6)
  }
}


