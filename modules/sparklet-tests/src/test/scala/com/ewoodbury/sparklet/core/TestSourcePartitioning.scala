package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection

class TestSourcePartitioning extends AnyFlatSpec with Matchers {

  private def partitionSizes[A](dc: DistCollection[A]): Seq[Int] = dc.plan match {
    case Plan.Source(partitions) => partitions.map(partition => partition.data.size).toSeq
    case other => fail(s"Expected Plan.Source, got $other")
  }

  "DistCollection.apply" should "produce exactly the requested number of balanced partitions" in {
    val dc = DistCollection(Seq(1, 2, 3, 4, 5), 3)

    partitionSizes(dc) shouldEqual Seq(2, 2, 1)
  }

  it should "produce exactly N empty partitions for empty input" in {
    val dc = DistCollection(Seq.empty[Int], 3)

    partitionSizes(dc) shouldEqual Seq(0, 0, 0)
  }

  it should "produce N partitions when records are fewer than partitions" in {
    val dc = DistCollection(Seq(1, 2), 4)

    partitionSizes(dc) shouldEqual Seq(1, 1, 0, 0)
  }

  it should "distribute evenly divisible input into equal partitions" in {
    val dc = DistCollection(Seq(1, 2, 3, 4), 2)

    partitionSizes(dc) shouldEqual Seq(2, 2)
  }

  it should "preserve input order within partitions and across partitions" in {
    val data = (1 to 100)
    val dc = DistCollection(data, 7)

    dc.collect() shouldEqual data
  }

  it should "place earlier records in earlier partitions" in {
    val dc = DistCollection(Seq(1, 2, 3, 4, 5), 2)

    dc.plan match {
      case Plan.Source(partitions) =>
        partitions.head.data.toSeq shouldEqual Seq(1, 2, 3)
        partitions.drop(1).head.data.toSeq shouldEqual Seq(4, 5)
      case other => fail(s"Expected Plan.Source, got $other")
    }
  }

  it should "reject non-positive partition counts" in {
    an[IllegalArgumentException] should be thrownBy DistCollection(Seq(1, 2), 0)
    an[IllegalArgumentException] should be thrownBy DistCollection(Seq(1, 2), -2)
  }

  it should "support execution over empty partitions end to end" in {
    val dc = DistCollection(Seq.empty[(String, Int)], 3)

    dc.collect() shouldEqual Seq.empty
    dc.count() shouldEqual 0L
    dc.map(pair => pair._2).collect() shouldEqual Seq.empty
    dc.filter(pair => pair._2 > 0).collect() shouldEqual Seq.empty
    dc.groupByKey.collect() shouldEqual Seq.empty
  }

  it should "support a single element in many partitions" in {
    val dc = DistCollection(Seq(42), 4)

    partitionSizes(dc) shouldEqual Seq(1, 0, 0, 0)
    dc.collect() shouldEqual Seq(42)
  }
}
