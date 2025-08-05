package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// import com.ewoodbury.sparklet.core.{DistCollection, Plan, Partition}

class TestLocalActions extends AnyFlatSpec with Matchers {
  val toDistCollection: [T] => (seq: Seq[T]) => DistCollection[T] = [T] => (seq: Seq[T]) => DistCollection(com.ewoodbury.sparklet.core.Plan.Source(Seq(Partition(seq))))

  "Executor" should "execute a simple collect operation" in {
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val result = source.collect()
    val expected = Seq(1, 2, 3, 4, 5)

    result shouldEqual expected
  }

  it should "execute a simple take operation" in {
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val result = source.take(3)
    val expected = Seq(1, 2, 3)

    result shouldEqual expected
  }

  it should "execute a simple first operation" in {
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val first = source.first()
    val result = first
    val expected = 1

    result shouldEqual expected
  }

  it should "execute a simple reduce operation" in {
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val reduced = source.reduce(_ + _)
    val result = reduced
    val expected = 15

    result shouldEqual expected
  }

  it should "execute a simple reduce operation with strings" in {
    val source = toDistCollection(Seq("a", "b", "c"))
    val reduced = source.reduce(_ + _)
    val result = reduced
    val expected = "abc"

    result shouldEqual expected
  }

  it should "execute a simple fold operation" in {
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val folded = source.fold(0)(_ + _)
    val result = folded
    val expected = 15

    result shouldEqual expected
  }

  it should "execute a simple aggregate operation" in {
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val aggregated = source.aggregate(0)(_ + _, _ + _)
    val result = aggregated
    val expected = 15

    result shouldEqual expected
  }

  it should "execute a complex aggregate operation for sum and count" in {
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))
    val aggregated = source.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
    val result = aggregated
    val expected = (15, 5)

    result shouldEqual expected
  }

  
  it should "execute a simple foreach operation" in {
    val source = toDistCollection(Seq(1, 2, 3, 4, 5))

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var sum = 0
    
    val _ = source.foreach(sum += _)
    val result = sum
    val expected = 15

    result shouldEqual expected
  }
}