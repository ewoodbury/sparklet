package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.Partition

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

  "KeysTask" should "extract keys from partition data" in {
    val partition = Partition(Seq(1 -> "one", 2 -> "two"))
    val keysTask = Task.KeysTask(partition)
    val result = keysTask.run()
    
    result.data shouldEqual Seq(1, 2)
  }

  it should "handle empty partition" in {
    val partition = Partition(Seq.empty[(Int, String)])
    val keysTask = Task.KeysTask(partition)
    val result = keysTask.run()
    
    result.data shouldEqual Seq.empty[Int]
  }

  "ValuesTask" should "extract values from partition data" in {
    val partition = Partition(Seq(1 -> "one", 2 -> "two"))
    val valuesTask = Task.ValuesTask(partition)
    val result = valuesTask.run()
    
    result.data shouldEqual Seq("one", "two")
  }

  it should "handle empty partition" in {
    val partition = Partition(Seq.empty[(Int, String)])
    val valuesTask = Task.ValuesTask(partition)
    val result = valuesTask.run()
    
    result.data shouldEqual Seq.empty[String]
  }

  "MapValuesTask" should "apply a mapValues function to partition data" in {
    val partition = Partition(Seq(1 -> "one", 2 -> "two"))
    val mapValuesTask = Task.MapValuesTask(partition, (v: String) => v.toUpperCase(java.util.Locale.ENGLISH))
    val result = mapValuesTask.run()
    
    result.data shouldEqual Seq(1 -> "ONE", 2 -> "TWO")
  }

  it should "handle empty partition" in {
    val partition = Partition(Seq.empty[(Int, String)])
    val mapValuesTask = Task.MapValuesTask(partition, (v: String) => v.toUpperCase(java.util.Locale.ENGLISH))
    val result = mapValuesTask.run()
    
    result.data shouldEqual Seq.empty[Int]
  }

  "FilterKeysTask" should "filter keys from partition data" in {
    val partition = Partition(Seq(1 -> "one", 2 -> "two"))
    val filterKeysTask = Task.FilterKeysTask(partition, (k: Int) => k % 2 == 0)
    val result = filterKeysTask.run()
    
    result.data shouldEqual Seq(2 -> "two")
  }

  it should "handle empty partition" in {
    val partition = Partition(Seq.empty[(Int, String)])
    val filterKeysTask = Task.FilterKeysTask(partition, (k: Int) => k % 2 == 0)
    val result = filterKeysTask.run()
    
    result.data shouldEqual Seq.empty[(Int, String)]
  }

  "FilterValuesTask" should "filter values from partition data" in {
    val partition = Partition(Seq(1 -> "one", 2 -> "two", 3 -> "three"))
    val filterValuesTask = Task.FilterValuesTask(partition, (v: String) => v.length > 3)
    val result = filterValuesTask.run()
    
    result.data shouldEqual Seq(3 -> "three")
  }

  it should "handle empty partition" in {
    val partition = Partition(Seq.empty[(Int, String)])
    val filterValuesTask = Task.FilterValuesTask(partition, (v: String) => v.length > 3)
    val result = filterValuesTask.run()
    
    result.data shouldEqual Seq.empty[Int]
  }

  "FlatMapValuesTask" should "apply a flatMapValues function to partition data" in {
    val partition = Partition(Seq(1 -> "one", 2 -> "two"))
    val flatMapValuesTask = Task.FlatMapValuesTask(partition, (v: String) => Seq(v.toUpperCase(java.util.Locale.ENGLISH)))
    val result = flatMapValuesTask.run()
    
    result.data shouldEqual Seq(1 -> "ONE", 2 -> "TWO")
  }

  it should "flatten nested collections" in {
    val partition = Partition(Seq(1 -> Seq("one", "two"), 2 -> Seq("three", "four")))
    val flatMapValuesTask = Task.FlatMapValuesTask(partition, (v: Seq[String]) => v)
    val result = flatMapValuesTask.run()
    
    result.data shouldEqual Seq(1 -> "one", 1 -> "two", 2 -> "three", 2 -> "four")
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