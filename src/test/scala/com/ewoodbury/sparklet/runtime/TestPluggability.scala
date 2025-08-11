package com.ewoodbury.sparklet.runtime

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{DistCollection, Partition, PartitionId, Plan}
import com.ewoodbury.sparklet.execution.Task
import com.ewoodbury.sparklet.runtime.api.*

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class TestPluggability extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  private var original: Option[SparkletRuntime.RuntimeComponents] = None

  override def beforeEach(): Unit = {
    original = Some(SparkletRuntime.get)
  }

  override def afterEach(): Unit = {
    SparkletRuntime.clearForCurrentThread()
    original.foreach(SparkletRuntime.set)
  }

  private final class RecordingScheduler extends TaskScheduler {
    @volatile var lastSubmittedCount: Int = 0
    def submit[A, B](tasks: Seq[Task[A, B]]): Seq[Partition[B]] = {
      lastSubmittedCount = tasks.size
      tasks.map(_.run())
    }
    def shutdown(): Unit = ()
  }

  private final class RecordingShuffle extends ShuffleService {
    import ShuffleService.ShuffleData
    private val underlying = com.ewoodbury.sparklet.runtime.local.LocalShuffleService()
    @volatile var partitionCalls: Int = 0
    @volatile var writeCalls: Int = 0

    def partitionByKey[K, V](
        data: Seq[Partition[(K, V)]],
        numPartitions: Int,
        partitioner: Partitioner,
    ): ShuffleData[K, V] = {
      partitionCalls += 1
      underlying.partitionByKey(data, numPartitions, partitioner)
    }

    def write[K, V](shuffleData: ShuffleData[K, V]) = {
      writeCalls += 1
      underlying.write(shuffleData)
    }

    def readPartition[K, V](id: com.ewoodbury.sparklet.core.ShuffleId, partitionId: PartitionId) =
      underlying.readPartition(id, partitionId)

    def partitionCount(id: com.ewoodbury.sparklet.core.ShuffleId): Int = underlying.partitionCount(id)
    def clear(): Unit = underlying.clear()
  }

  it should "route narrow plans through the runtime TaskScheduler" in {
    val recSched = RecordingScheduler()
    val recShuffle = RecordingShuffle()
    SparkletRuntime.setForCurrentThread(SparkletRuntime.RuntimeComponents(
      scheduler = recSched,
      executor = com.ewoodbury.sparklet.runtime.local.LocalExecutorBackend(),
      shuffle = recShuffle,
      partitioner = com.ewoodbury.sparklet.runtime.local.HashPartitioner(),
    ))

    val dc = DistCollection(Plan.Source(Seq(Partition(Seq(1, 2, 3)))))
      .map(_ * 2)
      .filter(_ > 2)

    val res = dc.collect().toSeq
    res should contain theSameElementsAs Seq(4, 6)
    recSched.lastSubmittedCount should be > 0
    recShuffle.partitionCalls shouldEqual 0 // no shuffle for narrow plan
    recShuffle.writeCalls shouldEqual 0
  }

  it should "route shuffle plans through the injected ShuffleService" in {
    val recSched = RecordingScheduler()
    val recShuffle = RecordingShuffle()
    SparkletRuntime.setForCurrentThread(SparkletRuntime.RuntimeComponents(
      scheduler = recSched,
      executor = com.ewoodbury.sparklet.runtime.local.LocalExecutorBackend(),
      shuffle = recShuffle,
      partitioner = com.ewoodbury.sparklet.runtime.local.HashPartitioner(),
    ))

    val dc = DistCollection(Plan.Source(Seq(Partition(Seq("a" -> 1, "a" -> 2, "b" -> 3)))))
      .groupByKey[String, Int]

    val res = dc.collect().toMap
    res("a").toSeq should contain theSameElementsAs Seq(1, 2)
    res("b").toSeq should contain theSameElementsAs Seq(3)
    recShuffle.partitionCalls should be > 0
    recShuffle.writeCalls should be > 0
  }
}

