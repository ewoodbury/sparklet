package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterEach}

import com.ewoodbury.sparklet.core.{DistCollection, Partition, Plan}

/**
  * Tests for Executor.createTasks()
  *
  * This tests the creation of tasks from a plan.
  * It also tests the execution of tasks.
  * 
  * Note that this only tests the task layer in isolation (Layer 2 from architecture guide).
  * This is not test the full system, and hence these tests do not reflect how a user would write code!
  *
  * It is important to note that the createTasks() method is not a full DAG scheduler.
  * It is a simple scheduler that only handles a single stage of narrow transformations.
  */
class TestExecutorCreateTasks extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    // Clear shuffle data between tests to avoid interference
    ShuffleManager.clear()
  }

  /**
   * Helper function to create a DistCollection from a sequence for testing
   */
  val toDistCollection: [T] => (seq: Seq[T]) => DistCollection[T] = [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))

  "Executor.createTasks" should "create StageTask for MapOp plans" in {
    // Given: A simple map operation plan
    val sourcePlan = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val mapPlan = Plan.MapOp(sourcePlan, (x: Int) => x * 2)
    
    // When: We create tasks from the plan
    val tasks = Executor.createTasks(mapPlan)

    println(s"tasks: $tasks")
    
    // Then: We should get one StageTask per input partition
    tasks should have length 1
    tasks.headOption shouldBe a[Some[Task.StageTask[_, _]]]
    
    val stageTask = tasks.headOption.get.asInstanceOf[Task.StageTask[Int, Int]]
    stageTask.partition.data should contain theSameElementsAs Seq(1, 2, 3)
    
    // Verify the task can be executed and produces the correct map result
    val result = stageTask.run()
    result.data should contain theSameElementsAs Seq(2, 4, 6)
  }

  it should "create StageTask for FilterOp plans" in {
    // Given: A simple filter operation plan
    val sourcePlan = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val filterPlan = Plan.FilterOp(sourcePlan, (x: Int) => x > 2)
    
    // When: We create tasks from the plan
    val tasks = Executor.createTasks(filterPlan)

    println(s"tasks: $tasks")
    
    // Then: We should get one StageTask per input partition
    tasks should have length 1
    tasks.headOption shouldBe a[Some[Task.StageTask[_, _]]]

    val stageTask = tasks.headOption.get.asInstanceOf[Task.StageTask[Int, Int]]
    stageTask.partition.data should contain theSameElementsAs Seq(1, 2, 3)
    
    // Verify the task can be executed and produces the correct filter result
    val result = stageTask.run()
    result.data should contain theSameElementsAs Seq(3)
  }

  it should "create StageTask for chained Map and Filter operations" in {
    // Given: A chained map and filter operation plan
    val sourcePlan = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val mapPlan = Plan.MapOp(sourcePlan, (x: Int) => x * 2)
    val filterPlan = Plan.FilterOp(mapPlan, (x: Int) => x > 2)
    
    // When: We create tasks from the plan
    val tasks = Executor.createTasks(filterPlan)

    println(s"tasks: $tasks")
    
    // Then: We should get one StageTask per input partition that chains both operations
    tasks should have length 1
    tasks.headOption shouldBe a[Some[Task.StageTask[_, _]]]

    val stageTask = tasks.headOption.get.asInstanceOf[Task.StageTask[Int, Int]]
    stageTask.partition.data should contain theSameElementsAs Seq(1, 2, 3)
    
    // Verify the task executes the entire chain: map (x2) then filter (>2)
    // [1,2,3] -> map -> [2,4,6] -> filter -> [4,6]
    val result = stageTask.run()
    result.data should contain theSameElementsAs Seq(4, 6)
  }

  it should "create StageTask for FlatMapOp plans" in {
    // Given: A simple flatMap operation plan
    val sourcePlan = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val flatMapPlan = Plan.FlatMapOp(sourcePlan, (x: Int) => Seq(x, x * 2))
    
    // When: We create tasks from the plan
    val tasks = Executor.createTasks(flatMapPlan)
    
    // Then: We should get one StageTask per input partition
    tasks should have length 1
    tasks.headOption shouldBe a[Some[Task.StageTask[_, _]]]

    val stageTask = tasks.headOption.get.asInstanceOf[Task.StageTask[Int, Int]]
    stageTask.partition.data should contain theSameElementsAs Seq(1, 2, 3)
    
    // Verify the task can be executed and produces the correct flatMap result
    val result = stageTask.run()
    result.data should contain theSameElementsAs Seq(1, 2, 2, 4, 3, 6)
  }

  it should "support GroupByKeyOp shuffle operations" in {
    // Given: A GroupByKey operation plan (shuffle operation)
    val sourcePlan = Plan.Source(Seq(Partition(Seq(("a", 1), ("b", 2), ("a", 3), ("c", 4)))))
    val groupByKeyPlan = Plan.GroupByKeyOp(sourcePlan)

    // When: We create tasks from the plan
    val tasks = Executor.createTasks(groupByKeyPlan)

    println(s"tasks: $tasks")

    // Then: We should get one DAGTask for shuffle operations
    tasks should have length 1
    tasks.headOption.get shouldBe a[Task.DAGTask[_]]
    
    // Execute the task and verify it produces correct groupByKey results
    val result = tasks.headOption.get.run()
    
    // Expected: ("a" -> [1,3]), ("b" -> [2]), ("c" -> [4])
    val resultData = result.data.asInstanceOf[Seq[(String, Iterable[Int])]]
    resultData should have length 3
    
    val resultMap = resultData.toMap
    resultMap("a") should contain theSameElementsAs Seq(1, 3)
    resultMap("b") should contain theSameElementsAs Seq(2)
    resultMap("c") should contain theSameElementsAs Seq(4)
  }

  it should "support ReduceByKeyOp shuffle operations" in {
    // Given: A ReduceByKey operation plan (shuffle operation)
    val sourcePlan = Plan.Source(Seq(Partition(Seq(("a", 1), ("b", 2), ("a", 3), ("c", 4)))))
    val reduceByKeyPlan = Plan.ReduceByKeyOp(sourcePlan, (x: Int, y: Int) => x + y)

    // When: We create tasks from the plan
    val tasks = Executor.createTasks(reduceByKeyPlan)

    println(s"reduceByKey tasks: $tasks")

    // Then: We should get one DAGTask for shuffle operations
    tasks should have length 1
    tasks.headOption.get shouldBe a[Task.DAGTask[_]]
    
    // Execute the task and verify it produces correct reduceByKey results
    val result = tasks.headOption.get.run()
    
    // Expected: ("a" -> 4), ("b" -> 2), ("c" -> 4) where values are summed
    val resultData = result.data.asInstanceOf[Seq[(String, Int)]]
    resultData should have length 3
    
    val resultMap = resultData.toMap
    resultMap("a") shouldBe 4 // 1 + 3
    resultMap("b") shouldBe 2 // 2
    resultMap("c") shouldBe 4 // 4
  }

  it should "support SortByOp shuffle operations" in {
    // Given: A SortBy operation plan (shuffle operation)
    val sourcePlan = Plan.Source(Seq(Partition(Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4)))))
    val sortByPlan = Plan.SortByOp(sourcePlan, (x: (String, Int)) => x._2, Ordering.Int.reverse)

    // When: We create tasks from the plan
    val tasks = Executor.createTasks(sortByPlan)

    // Then: We should get one DAGTask for shuffle operations
    tasks should have length 1
    tasks.headOption.get shouldBe a[Task.DAGTask[_]]

    // Execute the task and verify it produces correct sortBy results
    val result = tasks.headOption.get.run()
    
    // Expected: All 4 elements sorted by value in descending order
    // [("d", 4), ("c", 3), ("b", 2), ("a", 1)]
    val resultData = result.data.asInstanceOf[Seq[(String, Int)]]
    resultData should have length 4
    
    // Verify the elements are in descending order by their integer values
    resultData shouldBe Seq(("d", 4), ("c", 3), ("b", 2), ("a", 1))
  }
  
}