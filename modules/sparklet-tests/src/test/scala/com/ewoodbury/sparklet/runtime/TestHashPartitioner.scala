package com.ewoodbury.sparklet.runtime

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.runtime.local.HashPartitioner

class TestHashPartitioner extends AnyFlatSpec with Matchers {

  private val partitioner = new HashPartitioner

  "HashPartitioner" should "handle Int.MinValue hashes without producing negative partitions" in {
    val partition = partitioner.partition(Int.MinValue, 4)

    partition should be >= 0
    partition should be < 4
  }

  it should "handle arbitrary negative hash codes" in {
    val negativeHashKeys: Seq[Any] = Seq(-1, -42, -1000, Int.box(-7))

    negativeHashKeys.foreach { key =>
      val partition = partitioner.partition(key, 8)
      partition should be >= 0
      partition should be < 8
    }
  }

  it should "always return a valid partition for keys with negative hash codes" in {
    val keys = (-1000 until 1000).map(i => new AnyRef {
      override def hashCode(): Int = i
    })

    keys.foreach { key =>
      val partition = partitioner.partition(key, 5)
      partition should be >= 0
      partition should be < 5
    }
  }

  it should "map all keys to the single partition when numPartitions is 1" in {
    val keys = Seq("a", "b", 1, 2.5, Int.MinValue, -12345)

    keys.foreach { key =>
      partitioner.partition(key, 1) shouldEqual 0
    }
  }

  it should "return 0 for non-positive partition counts" in {
    partitioner.partition("a", 0) shouldEqual 0
    partitioner.partition("a", -3) shouldEqual 0
  }

  it should "distribute distinct keys across partitions" in {
    val keys = (0 until 100).map(i => s"key-$i")
    val usedPartitions = keys.map(partitioner.partition(_, 4)).toSet

    usedPartitions.size should be > 1
  }
}
