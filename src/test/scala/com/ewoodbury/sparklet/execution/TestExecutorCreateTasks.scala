package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
class TestExecutorCreateTasks extends AnyFlatSpec with Matchers {
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

    println(s"tasks: $tasks")
    
    // Then: We should get one StageTask per input partition
    tasks should have length 1
    tasks.headOption shouldBe a[Some[Task.StageTask[_, _]]]

    val stageTask = tasks.headOption.get.asInstanceOf[Task.StageTask[Int, Int]]
    stageTask.partition.data should contain theSameElementsAs Seq(1, 2, 3)
    
    // Verify the task can be executed and produces the correct flatMap result
    val result = stageTask.run()
    result.data should contain theSameElementsAs Seq(1, 2, 2, 4, 3, 6)
  }

  // TODO: Add tests for other operations
  
}