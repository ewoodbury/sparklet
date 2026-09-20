package com.ewoodbury.sparklet.core

import java.util.concurrent.CountDownLatch

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.execution.{Stage, Task}

class TestPartitionSemantics extends AnyFlatSpec with Matchers {

  "Partition data" should "be re-traversable without mutating the source" in {
    val partition = Partition(Seq(1, 2, 3))

    partition.data.toSeq shouldEqual Seq(1, 2, 3)
    partition.data.toSeq shouldEqual Seq(1, 2, 3)
  }

  it should "survive repeated task execution over the same partition" in {
    // The task scheduler may re-run a task (retry); each run must see the full input
    val partition = Partition(Seq(1, 2, 3, 4))
    val task = Task.StageTask(partition, Stage.filter((x: Int) => x % 2 == 0))

    task.run().data.toSeq shouldEqual Seq(2, 4)
    task.run().data.toSeq shouldEqual Seq(2, 4)
  }

  it should "stay intact after an action consumes the collection" in {
    // LazyList-backed partitions memoize; earlier actions must not drain later ones
    val dc = DistCollection(1 to 100, 4)

    dc.count() shouldEqual 100L
    dc.collect().toSeq shouldEqual (1 to 100)
    dc.aggregate(0)(_ + _, _ + _) shouldEqual 5050
  }

  it should "produce identical results when traversed concurrently" in {
    val partition = Partition(1 to 1000)
    val results = new Array[Seq[Int]](8)
    val ready = new CountDownLatch(1)
    val done = new CountDownLatch(8)

    (0 until 8).foreach { i =>
      new Thread(() => {
        ready.await()
        results(i) = partition.data.toSeq
        done.countDown()
      }).start()
    }
    ready.countDown()
    done.await()

    // Copies (LazyList memoization) must yield consistent snapshots across threads
    results.foreach { collected =>
      collected should contain theSameElementsInOrderAs (1 to 1000)
    }
  }

  it should "support action-after-action on wide plans without losing data" in {
    val dc = DistCollection(Seq(("a", 1), ("b", 2), ("a", 3)), 2)

    val firstCount = dc.groupByKey.count()
    val collected = dc.groupByKey.collect().toMap
    val secondCount = dc.groupByKey.count()

    firstCount shouldEqual secondCount
    collected("a") should contain theSameElementsAs Seq(1, 3)
    collected("b") should contain theSameElementsAs Seq(2)
  }
}
