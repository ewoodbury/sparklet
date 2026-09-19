package com.ewoodbury.sparklet.runtime

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{BroadcastId, Partition, PartitionId, RetryPolicy, ShuffleId}
import com.ewoodbury.sparklet.runtime.api.*

class TestPluggability extends AnyFlatSpec with Matchers {

  behavior of "Sparklet runtime pluggability"

  it should "allow swapping scheduler, shuffle, partitioner, and broadcast implementations" in {
    // Fake scheduler that runs tasks sequentially
    val fakeScheduler = new TaskScheduler[IO] {
      def submit[A, B](tasks: Seq[RunnableTask[A, B]]): IO[Seq[Partition[B]]] =
        IO.pure(tasks.map(_.run()))

      def submitWithRetry[A, B](
        tasks: Seq[RunnableTask[A, B]],
        retryPolicy: RetryPolicy,
      ): IO[Seq[Partition[B]]] =
        submit(tasks) // Simple implementation for test

      def shutdown(): IO[Unit] = IO.unit
    }

    val fakePartitioner = new Partitioner { def partition(key: Any, n: Int): Int = 0 }

    val fakeShuffle = new ShuffleService {
      import ShuffleService.*
      def partitionByKey[K, V](data: Seq[Partition[(K, V)]], numPartitions: Int, partitioner: Partitioner): ShuffleData[K, V] =
        ShuffleData(Map.empty)
      def write[K, V](sd: ShuffleData[K, V]): ShuffleId = ShuffleId(0)
      def readPartition[K, V](id: ShuffleId, pid: PartitionId): Partition[(K, V)] =
        Partition(Seq.empty)
      def partitionCount(id: ShuffleId): Int = 0
      def clear(): Unit = ()
    }

    val fakeBroadcast: BroadcastService = new BroadcastService {
      def broadcast[T](data: Seq[T]): BroadcastId = BroadcastId(0)
      def getBroadcast[T](id: BroadcastId): Seq[T] = Seq.empty[T]
      def clear(): Unit = ()
    }

    val orig = SparkletRuntime.get
    SparkletRuntime.set(
      SparkletRuntime.RuntimeComponents(
        scheduler = fakeScheduler,
        shuffle = fakeShuffle,
        partitioner = fakePartitioner,
        broadcast = fakeBroadcast,
      )
    )

    try {
      val partition = Partition(Seq(1, 2, 3))
      val task = com.ewoodbury.sparklet.execution.Task.StageTask(partition, com.ewoodbury.sparklet.execution.Stage.map((x: Int) => x + 1))
      val result = SparkletRuntime.get.scheduler.submit(Seq(task)).unsafeRunSync()
      result.headOption.map(_.data.toSeq) shouldEqual Some(Seq(2, 3, 4))
    } finally {
      SparkletRuntime.set(orig)
    }
  }
}
