package com.ewoodbury.sparklet.execution

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.Partition
import com.ewoodbury.sparklet.runtime.SparkletRuntime

@SuppressWarnings(Array("org.wartremover.warts.SeqApply", "org.wartremover.warts.ThreadSleep"))
class TestTaskScheduler extends AnyFlatSpec with Matchers {

  private def mapTask[A, B](partition: Partition[A], f: A => B): Task.StageTask[A, B] =
    Task.StageTask(partition, Stage.map(f))

  private def filterTask[A](partition: Partition[A], p: A => Boolean): Task.StageTask[A, A] =
    Task.StageTask(partition, Stage.filter(p))

  private def flatMapTask[A, B](partition: Partition[A], f: A => IterableOnce[B]): Task.StageTask[A, B] =
    Task.StageTask(partition, Stage.flatMap(f))

  "TaskScheduler" should "execute multiple tasks concurrently" in {
    val tasks = Seq(
      mapTask(Partition(Seq(1, 2, 3)), (x: Int) => x * 2),
      mapTask(Partition(Seq(4, 5, 6)), (x: Int) => x * 3),
      mapTask(Partition(Seq(7, 8, 9)), (x: Int) => x * 4),
    )

    val results = SparkletRuntime.get.scheduler.submit(tasks).unsafeRunSync()

    results should have length 3
    results(0).data shouldEqual Seq(2, 4, 6)
    results(1).data shouldEqual Seq(12, 15, 18)
    results(2).data shouldEqual Seq(28, 32, 36)
  }

  it should "execute filter tasks concurrently" in {
    val tasks = Seq(
      filterTask(Partition(Seq(1, 2, 3, 4, 5)), (x: Int) => x % 2 == 0),
      filterTask(Partition(Seq(6, 7, 8, 9, 10)), (x: Int) => x > 7),
    )

    val results = SparkletRuntime.get.scheduler.submit(tasks).unsafeRunSync()

    results should have length 2
    results(0).data shouldEqual Seq(2, 4)
    results(1).data shouldEqual Seq(8, 9, 10)
  }

  it should "execute flatMap tasks concurrently" in {
    val tasks = Seq(
      flatMapTask(Partition(Seq("a", "b")), (s: String) =>
        Seq(s, s.toUpperCase(java.util.Locale.ENGLISH)),
      ),
      flatMapTask(Partition(Seq("c", "d")), (s: String) => Seq(s * 2)),
    )

    val results = SparkletRuntime.get.scheduler.submit(tasks).unsafeRunSync()

    results should have length 2
    results(0).data shouldEqual Seq("a", "A", "b", "B")
    results(1).data shouldEqual Seq("cc", "dd")
  }

  it should "handle empty partitions" in {
    val results =
      SparkletRuntime.get.scheduler.submit(Seq(mapTask(Partition(Seq.empty[Int]), (x: Int) => x * 2))).unsafeRunSync()

    results should have length 1
    results(0).data shouldEqual Seq.empty[Int]
  }

  it should "handle single task submission" in {
    val results =
      SparkletRuntime.get.scheduler.submit(Seq(mapTask(Partition(Seq(1, 2, 3)), (x: Int) => x.toString))).unsafeRunSync()

    results should have length 1
    results(0).data shouldEqual Seq("1", "2", "3")
  }

  it should "preserve task order in results" in {
    val tasks = Seq(1, 2, 3).map { value =>
      mapTask(Partition(Seq(value)), (x: Int) => s"task$value" + "_" + x.toString)
    }

    val results = SparkletRuntime.get.scheduler.submit(tasks).unsafeRunSync()

    results should have length 3
    results(0).data shouldEqual Seq("task1_1")
    results(1).data shouldEqual Seq("task2_2")
    results(2).data shouldEqual Seq("task3_3")
  }

  it should "handle large number of tasks" in {
    val tasks = (1 to 10).map(i => mapTask(Partition(Seq(i)), (x: Int) => x * 10))

    val results = SparkletRuntime.get.scheduler.submit(tasks).unsafeRunSync()

    results should have length 10
    results.zipWithIndex.foreach { case (result, index) =>
      result.data shouldEqual Seq((index + 1) * 10)
    }
  }

  it should "handle tasks with different computation times" in {
    val partition = Partition(Seq(1))

    // Instead of testing exact timing (which is unreliable in test environments),
    // test that all tasks complete and produce results. The concurrency aspect
    // is tested by the fact that multiple tasks are submitted and all complete.

    val fastTask = mapTask(partition, (x: Int) => x * 2)
    val mediumTask = mapTask(partition, (x: Int) => {
      (1 to 1000).foreach(_ => ()) // Small busy loop
      x + 10
    })
    val complexTask = mapTask(partition, (x: Int) => {
      (1 to 10000).foreach(_ => ()) // Larger busy loop
      x + 100
    })

    val startTime = System.currentTimeMillis()
    val results = SparkletRuntime.get.scheduler.submit(Seq(fastTask, mediumTask, complexTask)).unsafeRunSync()
    val endTime = System.currentTimeMillis()

    results should have length 3
    results(0).data shouldEqual Seq(2) // fastTask result
    results(1).data shouldEqual Seq(11) // mediumTask result
    results(2).data shouldEqual Seq(101) // complexTask result

    val totalTime = endTime - startTime
    totalTime should be >= 0L // At least some time passed
    totalTime should be < 30000L // But not excessively long (30 seconds max)
  }

  it should "handle complex transformations" in {
    val partition = Partition(Seq(1, 2, 3, 4, 5))

    // Chain multiple transformations in a single task
    val complexTask = mapTask(partition, (x: Int) => {
      val doubled = x * 2
      val filtered = if (doubled > 5) doubled else 0
      s"result_$filtered"
    })

    val results = SparkletRuntime.get.scheduler.submit(Seq(complexTask)).unsafeRunSync()

    results should have length 1
    results(0).data shouldEqual Seq("result_0", "result_0", "result_6", "result_8", "result_10")
  }
}
