package com.ewoodbury.sparklet.localengine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestTask extends AnyFlatSpec with Matchers {
  
  "MapTask" should "apply a map function to partition data" in {
    val partition = Partition(Seq(1, 2, 3, 4, 5))
    val mapTask = Task.MapTask(partition, (x: Int) => x * 2)
    val result = mapTask.run()
    
    result.data shouldEqual Seq(2, 4, 6, 8, 10)
  }

  it should "handle empty partition" in {
    val partition = Partition(Seq.empty[Int])
    val mapTask = Task.MapTask(partition, (x: Int) => x.toString)
    val result = mapTask.run()
    
    result.data shouldEqual Seq.empty[String]
  }

  it should "apply string transformation" in {
    val partition = Partition(Seq("hello", "world"))
    val mapTask = Task.MapTask(partition, (s: String) => s.toUpperCase(java.util.Locale.ENGLISH))
    val result = mapTask.run()
    
    result.data shouldEqual Seq("HELLO", "WORLD")
  }

  "FilterTask" should "filter partition data based on predicate" in {
    val partition = Partition(Seq(1, 2, 3, 4, 5, 6))
    val filterTask = Task.FilterTask(partition, (x: Int) => x % 2 == 0)
    val result = filterTask.run()
    
    result.data shouldEqual Seq(2, 4, 6)
  }

  it should "handle empty partition" in {
    val partition = Partition(Seq.empty[Int])
    val filterTask = Task.FilterTask(partition, (x: Int) => x > 0)
    val result = filterTask.run()
    
    result.data shouldEqual Seq.empty[Int]
  }

  it should "filter all elements out" in {
    val partition = Partition(Seq(1, 2, 3, 4, 5))
    val filterTask = Task.FilterTask(partition, (x: Int) => x > 10)
    val result = filterTask.run()
    
    result.data shouldEqual Seq.empty[Int]
  }

  it should "keep all elements" in {
    val partition = Partition(Seq(1, 2, 3, 4, 5))
    val filterTask = Task.FilterTask(partition, (x: Int) => x > 0)
    val result = filterTask.run()
    
    result.data shouldEqual Seq(1, 2, 3, 4, 5)
  }

  "FlatMapTask" should "apply flatMap function to partition data" in {
    val partition = Partition(Seq(1, 2, 3))
    val flatMapTask = Task.FlatMapTask(partition, (x: Int) => Seq(x, x * 2))
    val result = flatMapTask.run()
    
    result.data shouldEqual Seq(1, 2, 2, 4, 3, 6)
  }

  it should "handle empty partition" in {
    val partition = Partition(Seq.empty[Int])
    val flatMapTask = Task.FlatMapTask(partition, (x: Int) => Seq(x.toString))
    val result = flatMapTask.run()
    
    result.data shouldEqual Seq.empty[String]
  }

  it should "produce empty results for some elements" in {
    val partition = Partition(Seq(1, 2, 3, 4))
    val flatMapTask = Task.FlatMapTask(partition, (x: Int) => 
      if (x % 2 == 0) Seq(x) else Seq.empty[Int]
    )
    val result = flatMapTask.run()
    
    result.data shouldEqual Seq(2, 4)
  }

  it should "produce multiple elements per input" in {
    val partition = Partition(Seq("a", "b"))
    val flatMapTask = Task.FlatMapTask(partition, (s: String) => 
      s.toUpperCase(java.util.Locale.ENGLISH).map(_.toString)
    )
    val result = flatMapTask.run()
    
    result.data shouldEqual Seq("A", "B")
  }

  "FlatMapTask" should "handle nested collections" in {
    val partition = Partition(Seq(Seq(1, 2), Seq(3, 4)))
    val flatMapTask = Task.FlatMapTask(partition, (x: Seq[Int]) => x)
    val result = flatMapTask.run()
    
    result.data shouldEqual Seq(1, 2, 3, 4)
  }

  it should "handle empty nested collections" in {
    val partition = Partition(Seq(Seq.empty[Int], Seq(1, 2)))
    val flatMapTask = Task.FlatMapTask(partition, (x: Seq[Int]) => x)
    val result = flatMapTask.run()
    
    result.data shouldEqual Seq(1, 2)
  }
  
  "DistinctTask" should "remove duplicates from partition data" in {
    val partition = Partition(Seq(1, 2, 3, 2, 4, 1))
    val distinctTask = Task.DistinctTask(partition)
    val result = distinctTask.run()
    
    result.data shouldEqual Seq(1, 2, 3, 4)
  }

  it should "handle empty partition" in {
    val partition = Partition(Seq.empty[Int])
    val distinctTask = Task.DistinctTask(partition)
    val result = distinctTask.run()
    
    result.data shouldEqual Seq.empty[Int]
  }

  "All Tasks" should "preserve partition structure" in {
    val originalPartition = Partition(Seq(1, 2, 3))
    
    val mapTask = Task.MapTask(originalPartition, (x: Int) => x * 2)
    val mapResult = mapTask.run()
    mapResult shouldBe a[Partition[_]]
    
    val filterTask = Task.FilterTask(originalPartition, (x: Int) => x > 1)
    val filterResult = filterTask.run()
    filterResult shouldBe a[Partition[_]]
    
    val flatMapTask = Task.FlatMapTask(originalPartition, (x: Int) => Seq(x))
    val flatMapResult = flatMapTask.run()
    flatMapResult shouldBe a[Partition[_]]
  }
}