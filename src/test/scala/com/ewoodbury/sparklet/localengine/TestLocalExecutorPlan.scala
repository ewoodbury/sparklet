package com.ewoodbury.sparklet.localengine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Tests for LocalExecutor.createTasks()
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
class TestLocalExecutorPlan extends AnyFlatSpec with Matchers {
  /**
   * Helper function to create a DistCollection from a sequence for testing
   */
  val toDistCollection = [T] => (seq: Seq[T]) => DistCollection(Plan.Source(Seq(Partition(seq))))

  "LocalExecutor.createTasks" should "create MapTask for MapOp plans" in {
    // Given: A simple map operation plan
    val sourcePlan = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val mapPlan = Plan.MapOp(sourcePlan, (x: Int) => x * 2)
    
    // When: We create tasks from the plan
    val tasks = LocalExecutor.createTasks(mapPlan)

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
    val sourcePlan = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val filterPlan = Plan.FilterOp(sourcePlan, (x: Int) => x > 2)
    
    // When: We create tasks from the plan
    val tasks = LocalExecutor.createTasks(filterPlan)

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

  // TODO: Add tests for other operations
  
}