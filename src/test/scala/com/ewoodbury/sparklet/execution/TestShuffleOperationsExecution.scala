package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{DistCollection, Partition, Plan}
import com.ewoodbury.sparklet.runtime.api.SparkletRuntime

class TestShuffleOperationsExecution extends AnyFlatSpec with Matchers {
  val toDistCollection: [T] => (seq: Seq[T]) => DistCollection[T] = [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))

  "Shuffle operations execution" should "execute groupByKey operations correctly" in {
    SparkletRuntime.get.shuffle.clear() // Clean state
    val source = toDistCollection(Seq(1 -> "one", 2 -> "two", 1 -> "uno"))
    val result = source.groupByKey.collect()
    
    // Convert to map for easier testing (order doesn't matter)
    val resultMap = result.toMap
    
    resultMap should have size 2
    resultMap(1) should contain theSameElementsAs Seq("one", "uno")
    resultMap(2) should contain theSameElementsAs Seq("two")
  }

  it should "execute reduceByKey operations correctly" in {
    SparkletRuntime.get.shuffle.clear()
    val source = toDistCollection(Seq("one" -> 1, "one" -> 2, "two" -> 3, "one" -> 4))
    val result = source.reduceByKey((a: Int, b: Int) => a + b).collect()
    
    val resultMap = result.toMap
    
    resultMap should have size 2
    resultMap("one") shouldBe 7  // 1 + 2 + 4
    resultMap("two") shouldBe 3
  }

  it should "execute sortBy operations correctly" in {
    SparkletRuntime.get.shuffle.clear()
    val source = toDistCollection(Seq(3, 1, 4, 1, 5, 9, 2, 6))
    val result = source.sortBy(identity).collect()
    
    result.toSeq shouldBe Seq(1, 1, 2, 3, 4, 5, 6, 9)
  }

  it should "execute sortBy with custom key function correctly" in {
    SparkletRuntime.get.shuffle.clear()
    val source = toDistCollection(Seq("apple", "pie", "a", "bb"))
    val result = source.sortBy(_.length).collect()
    
    result.toSeq shouldBe Seq("a", "bb", "pie", "apple")
  }

  it should "handle empty collections in shuffle operations" in {
    SparkletRuntime.get.shuffle.clear()
    val emptySource = toDistCollection(Seq.empty[(String, Int)])
    
    val groupedResult = emptySource.groupByKey.collect()
    val reducedResult = emptySource.reduceByKey((a: Int, b: Int) => a + b).collect()
    
    groupedResult shouldBe empty
    reducedResult shouldBe empty
    
    val emptySort = toDistCollection(Seq.empty[Int])
    val sortedResult = emptySort.sortBy(identity).collect()
    sortedResult shouldBe empty
  }

  it should "handle single-element collections correctly" in {
    SparkletRuntime.get.shuffle.clear()
    val singleKV = toDistCollection(Seq("key" -> 42))
    val groupedResult = singleKV.groupByKey.collect().toMap
    val reducedResult = singleKV.reduceByKey((a: Int, b: Int) => a + b).collect().toMap
    
    groupedResult("key") should contain theSameElementsAs Seq(42)
    reducedResult("key") shouldBe 42
    
    val singleSort = toDistCollection(Seq(99))
    val sortedResult = singleSort.sortBy(identity).collect()
    sortedResult.toSeq shouldBe Seq(99)
  }

  it should "handle duplicate keys correctly in all shuffle operations" in {
    SparkletRuntime.get.shuffle.clear()
    val source = toDistCollection(Seq("x" -> 10, "y" -> 20, "x" -> 30, "y" -> 40, "x" -> 50))
    
    // Test groupByKey with duplicates
    val grouped = source.groupByKey.collect().toMap
    grouped("x") should contain theSameElementsAs Seq(10, 30, 50)
    grouped("y") should contain theSameElementsAs Seq(20, 40)
    
    // Test reduceByKey with duplicates (create new source since we consumed the first)
    val source2 = toDistCollection(Seq("x" -> 10, "y" -> 20, "x" -> 30, "y" -> 40, "x" -> 50))
    val reduced = source2.reduceByKey((a: Int, b: Int) => a + b).collect().toMap
    reduced("x") shouldBe 90  // 10 + 30 + 50
    reduced("y") shouldBe 60  // 20 + 40
  }

  it should "handle large datasets efficiently" in {
    SparkletRuntime.get.shuffle.clear()
    // Create a larger dataset to test performance and correctness
    val largeData = (1 to 100).map(i => (i % 10) -> i)
    val source = toDistCollection(largeData)
    
    val result = source.groupByKey.collect().toMap
    
    // Should have 10 groups (keys 0-9)
    result should have size 10
    
    // Each group should have 10 elements
    result.values.foreach(_.size shouldBe 10)
    
    // Verify a specific group
    result(0) should contain theSameElementsAs (10 to 100 by 10)
  }

  it should "execute operations with complex value types correctly" in {
    SparkletRuntime.get.shuffle.clear()
    case class User(name: String, age: Int)
    val users = Seq(
      "dept1" -> User("Alice", 25),
      "dept2" -> User("Bob", 30), 
      "dept1" -> User("Charlie", 35),
      "dept2" -> User("Diana", 28)
    )
    
    val source = toDistCollection(users)
    val result = source.groupByKey.collect().toMap
    
    result should have size 2
    result("dept1").map(_.name) should contain theSameElementsAs Seq("Alice", "Charlie")
    result("dept2").map(_.name) should contain theSameElementsAs Seq("Bob", "Diana")
  }

  it should "handle error conditions gracefully" in {
    SparkletRuntime.get.shuffle.clear()
    // Test with operations that might fail
    val source = toDistCollection(Seq("positive" -> 5, "zero" -> 0, "negative" -> -3))
    
    // This should work without throwing exceptions
    noException should be thrownBy {
      source.groupByKey.collect()
    }
    
    noException should be thrownBy {
      source.sortBy(_._2).collect()
    }
  }
} 