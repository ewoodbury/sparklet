package com.ewoodbury.sparklet.execution

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.Partition
import com.ewoodbury.sparklet.runtime.api.SparkletRuntime

@SuppressWarnings(Array("org.wartremover.warts.SeqApply", "org.wartremover.warts.ThreadSleep"))
class TestTaskScheduler extends AnyFlatSpec with Matchers {

  "TaskScheduler" should "execute multiple tasks concurrently" in {
    val partition1 = Partition(Seq(1, 2, 3))
    val partition2 = Partition(Seq(4, 5, 6))
    val partition3 = Partition(Seq(7, 8, 9))
    
    val mapTask1 = Task.MapTask(partition1, (x: Int) => x * 2)
    val mapTask2 = Task.MapTask(partition2, (x: Int) => x * 3)
    val mapTask3 = Task.MapTask(partition3, (x: Int) => x * 4)
    
    val tasks = Seq(mapTask1, mapTask2, mapTask3)
    val results = SparkletRuntime.get.scheduler.submit(tasks).unsafeRunSync()
    
    results should have length 3
    results(0).data shouldEqual Seq(2, 4, 6)
    results(1).data shouldEqual Seq(12, 15, 18)
    results(2).data shouldEqual Seq(28, 32, 36)
  }

  it should "execute filter tasks concurrently" in {
    val partition1 = Partition(Seq(1, 2, 3, 4, 5))
    val partition2 = Partition(Seq(6, 7, 8, 9, 10))
    
    val filterTask1 = Task.FilterTask(partition1, (x: Int) => x % 2 == 0)
    val filterTask2 = Task.FilterTask(partition2, (x: Int) => x > 7)
    
    val tasks = Seq(filterTask1, filterTask2)
    val results = SparkletRuntime.get.scheduler.submit(tasks).unsafeRunSync()
    
    results should have length 2
    results(0).data shouldEqual Seq(2, 4)
    results(1).data shouldEqual Seq(8, 9, 10)
  }

  it should "execute flatMap tasks concurrently" in {
    val partition1 = Partition(Seq("a", "b"))
    val partition2 = Partition(Seq("c", "d"))
    
    val flatMapTask1 = Task.FlatMapTask(partition1, (s: String) => Seq(s, s.toUpperCase(java.util.Locale.ENGLISH)))
    val flatMapTask2 = Task.FlatMapTask(partition2, (s: String) => Seq(s * 2))
    
    val tasks = Seq(flatMapTask1, flatMapTask2)
    val results = SparkletRuntime.get.scheduler.submit(tasks).unsafeRunSync()
    
    results should have length 2
    results(0).data shouldEqual Seq("a", "A", "b", "B")
    results(1).data shouldEqual Seq("cc", "dd")
  }

  it should "handle empty partitions" in {
    val emptyPartition = Partition(Seq.empty[Int])
    val mapTask = Task.MapTask(emptyPartition, (x: Int) => x * 2)
    
    val results = SparkletRuntime.get.scheduler.submit(Seq(mapTask)).unsafeRunSync()
    
    results should have length 1
    results(0).data shouldEqual Seq.empty[Int]
  }

  it should "handle single task submission" in {
    val partition = Partition(Seq(1, 2, 3))
    val mapTask = Task.MapTask(partition, (x: Int) => x.toString)
    
    val results = SparkletRuntime.get.scheduler.submit(Seq(mapTask)).unsafeRunSync()
    
    results should have length 1
    results(0).data shouldEqual Seq("1", "2", "3")
  }

  it should "preserve task order in results" in {
    val partition1 = Partition(Seq(1))
    val partition2 = Partition(Seq(2))
    val partition3 = Partition(Seq(3))
    
    val task1 = Task.MapTask(partition1, (x: Int) => s"task1_$x")
    val task2 = Task.MapTask(partition2, (x: Int) => s"task2_$x")
    val task3 = Task.MapTask(partition3, (x: Int) => s"task3_$x")
    
    val tasks = Seq(task1, task2, task3)
    val results = SparkletRuntime.get.scheduler.submit(tasks).unsafeRunSync()
    
    results should have length 3
    results(0).data shouldEqual Seq("task1_1")
    results(1).data shouldEqual Seq("task2_2")
    results(2).data shouldEqual Seq("task3_3")
  }

  it should "handle different task types separately" in {
    val partition1 = Partition(Seq(1, 2, 3, 4, 5))
    val partition2 = Partition(Seq("hello", "world", "test"))
    
    // Test map tasks
    val mapTask = Task.MapTask(partition1, (x: Int) => x * 2)
    val mapResults = SparkletRuntime.get.scheduler.submit(Seq(mapTask)).unsafeRunSync()
    mapResults(0).data shouldEqual Seq(2, 4, 6, 8, 10)
    
    // Test filter tasks
    val filterTask = Task.FilterTask(partition1, (x: Int) => x % 2 == 0)
    val filterResults = SparkletRuntime.get.scheduler.submit(Seq(filterTask)).unsafeRunSync()
    filterResults(0).data shouldEqual Seq(2, 4)
    
    // Test flatMap tasks
    val flatMapTask = Task.FlatMapTask(partition2, (s: String) => s.split(""))
    val flatMapResults = SparkletRuntime.get.scheduler.submit(Seq(flatMapTask)).unsafeRunSync()
    flatMapResults(0).data shouldEqual Seq("h", "e", "l", "l", "o", "w", "o", "r", "l", "d", "t", "e", "s", "t")
  }

  it should "handle large number of tasks" in {
    val partitions = (1 to 10).map(i => Partition(Seq(i)))
    val tasks = partitions.map(p => Task.MapTask(p, (x: Int) => x * 10))
    
    val results = SparkletRuntime.get.scheduler.submit(tasks).unsafeRunSync()
    
    results should have length 10
    results.zipWithIndex.foreach { case (result, index) =>
      result.data shouldEqual Seq((index + 1) * 10)
    }
  }

  it should "handle tasks with different computation times" in {
    val partition = Partition(Seq(1))
    
    // Simulate different computation times
    val fastTask = Task.MapTask(partition, (x: Int) => x * 2)
    val slowTask1 = Task.MapTask(partition, (x: Int) => {
      Thread.sleep(100) // Simulate slower computation
      x * 2
    })
    val slowTask2 = Task.MapTask(partition, (x: Int) => {
      Thread.sleep(100) // Simulate slower computation
      x * 3
    })
    
    val startTime = System.currentTimeMillis()
    val results = SparkletRuntime.get.scheduler.submit(Seq(fastTask, slowTask1, slowTask2)).unsafeRunSync()
    val endTime = System.currentTimeMillis()
    
    results should have length 3
    results(0).data shouldEqual Seq(2)
    results(1).data shouldEqual Seq(2)
    results(2).data shouldEqual Seq(3)
    
    // Both tasks should complete, and the total time should be roughly the time of the slowest task
    // (not the sum of both tasks, since they run concurrently)
    val totalTime = endTime - startTime
    totalTime should be >= 50L
    totalTime should be < 200L
  }

  it should "handle string transformations" in {
    val partition = Partition(Seq("hello", "world"))
    val mapTask = Task.MapTask(partition, (s: String) => s.toUpperCase(java.util.Locale.ENGLISH))
    
    val results = SparkletRuntime.get.scheduler.submit(Seq(mapTask)).unsafeRunSync()
    
    results should have length 1
    results(0).data shouldEqual Seq("HELLO", "WORLD")
  }

  it should "handle complex transformations" in {
    val partition = Partition(Seq(1, 2, 3, 4, 5))
    
    // Chain multiple transformations in a single task
    val complexTask = Task.MapTask(partition, (x: Int) => {
      val doubled = x * 2
      val filtered = if (doubled > 5) doubled else 0
      s"result_$filtered"
    })
    
    val results = SparkletRuntime.get.scheduler.submit(Seq(complexTask)).unsafeRunSync()
    
    results should have length 1
    results(0).data shouldEqual Seq("result_0", "result_0", "result_6", "result_8", "result_10")
  }
}


