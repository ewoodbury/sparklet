package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{DistCollection, Partition}

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
  val toDistCollection = [T] => (seq: Seq[T]) => DistCollection(com.ewoodbury.sparklet.core.Plan.Source(Seq(Partition(seq))))

  "Executor.createTasks" should "create MapTask for MapOp plans" in {
    // Given: A simple map operation plan
    val sourcePlan = com.ewoodbury.sparklet.core.Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val mapPlan = com.ewoodbury.sparklet.core.Plan.MapOp(sourcePlan, (x: Int) => x * 2)
    
    // When: We create tasks from the plan
    val tasks = Executor.createTasks(mapPlan)

    println(s"tasks: $tasks")
    
    // Then: We should get one MapTask per input partition
    tasks should have length 1
    tasks.headOption shouldBe a[Some[Task.MapTask[_, _]]]
    
    val mapTask = tasks.headOption.get.asInstanceOf[Task.MapTask[Int, Int]]
    mapTask.partition.data should contain theSameElementsAs Seq(1, 2, 3)
    
    // Verify the task can be executed
    val result = mapTask.run()
    result.data should contain theSameElementsAs Seq(2, 4, 6)
  }

  it should "create FilterTask for FilterOp plans" in {
    // Given: A simple filter operation plan
    val sourcePlan = com.ewoodbury.sparklet.core.Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val filterPlan = com.ewoodbury.sparklet.core.Plan.FilterOp(sourcePlan, (x: Int) => x > 2)
    
    // When: We create tasks from the plan
    val tasks = Executor.createTasks(filterPlan)

    println(s"tasks: $tasks")
    
    // Then: We should get one FilterTask per input partition
    tasks should have length 1
    tasks.headOption shouldBe a[Some[Task.FilterTask[_]]]

    val filterTask = tasks.headOption.get.asInstanceOf[Task.FilterTask[Int]]
    filterTask.partition.data should contain theSameElementsAs Seq(1, 2, 3)
    
    // Verify the task can be executed
    val result = filterTask.run()
    result.data should contain theSameElementsAs Seq(3)
  }

  // TODO: Reenable once multiple transformations are supported.
  ignore should "create a Plan with Map and Filter operations" in {
    // Given: A simple map and filter operation plan
    val sourcePlan = com.ewoodbury.sparklet.core.Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val mapPlan = com.ewoodbury.sparklet.core.Plan.MapOp(sourcePlan, (x: Int) => x * 2)
    val filterPlan = com.ewoodbury.sparklet.core.Plan.FilterOp(mapPlan, (x: Int) => x > 2)
    
    // When: We create tasks from the plan
    val tasks = Executor.createTasks(filterPlan)

    println(s"tasks: $tasks")
    
    // Then: We should get one FilterTask per input partition
    tasks should have length 1
    tasks.headOption shouldBe a[Some[Task.FilterTask[_]]]

    val filterTask = tasks.headOption.get.asInstanceOf[Task.FilterTask[Int]]
    filterTask.partition.data should contain theSameElementsAs Seq(1, 2, 3)
    
    // Verify the task can be executed
    val result = filterTask.run()
    result.data should contain theSameElementsAs Seq(3)
  }

  it should "create FlatMapTask for FlatMapOp plans" in {
    // Given: A simple flatMap operation plan
    val sourcePlan = com.ewoodbury.sparklet.core.Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val flatMapPlan = com.ewoodbury.sparklet.core.Plan.FlatMapOp(sourcePlan, (x: Int) => Seq(x, x * 2))
    
    // When: We create tasks from the plan
    val tasks = Executor.createTasks(flatMapPlan)

    println(s"tasks: $tasks")
    
    // Then: We should get one FlatMapTask per input partition
    tasks should have length 1
    tasks.headOption shouldBe a[Some[Task.FlatMapTask[_, _]]]

    val flatMapTask = tasks.headOption.get.asInstanceOf[Task.FlatMapTask[Int, Int]]
    flatMapTask.partition.data should contain theSameElementsAs Seq(1, 2, 3)
    
    // Verify the task can be executed
    val result = flatMapTask.run()
    result.data should contain theSameElementsAs Seq(1, 2, 2, 4, 3, 6)
  }

  // TODO: Add tests for other operations
  
}