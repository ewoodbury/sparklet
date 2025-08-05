package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{Partition, Plan}

@SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
/**
 * Tests for StageBuilder internal logic.
 * 
 * This tests the stage building functionality in isolation, focusing on:
 * - Stage graph construction
 * - Shuffle stage creation
 * - Stage dependencies
 * - Input source handling
 * 
 * Note that this tests the internal stage building logic (Layer 2 from architecture guide).
 * These tests focus on the correctness of stage graph construction rather than end-to-end execution.
 */
class TestStageBuilder extends AnyFlatSpec with Matchers {

  "StageBuilder.buildStageGraph" should "create a single stage for simple map operations" in {
    // Given: A simple map operation plan
    val sourcePlan = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val mapPlan = Plan.MapOp(sourcePlan, (x: Int) => x * 2)
    
    // When: We build the stage graph
    val stageGraph = StageBuilder.buildStageGraph(mapPlan)
    
    // Then: We should get a single stage with no dependencies
    stageGraph.stages should have size 1
    stageGraph.dependencies should have size 0
    stageGraph.finalStageId shouldBe 0
    
    val stageInfo = stageGraph.stages(0)
    stageInfo.id shouldBe 0
    stageInfo.isShuffleStage shouldBe false
    stageInfo.shuffleId shouldBe None
    stageInfo.inputSources should have length 1
    stageInfo.inputSources.head shouldBe a[StageBuilder.SourceInput]
  }

  it should "create shuffle stages for groupByKey operations" in {
    // Given: A groupByKey operation plan
    val sourcePlan = Plan.Source(Seq(Partition(Seq(("a", 1), ("b", 2), ("a", 3)))))
    val groupByKeyPlan = Plan.GroupByKeyOp(sourcePlan)
    
    // When: We build the stage graph
    val stageGraph = StageBuilder.buildStageGraph(groupByKeyPlan)
    
    // Then: We should get two stages - source stage and shuffle stage
    stageGraph.stages should have size 2
    stageGraph.dependencies should have size 1
    stageGraph.finalStageId shouldBe 1
    
    // Check source stage (stage 0)
    val sourceStage = stageGraph.stages(0)
    sourceStage.id shouldBe 0
    sourceStage.isShuffleStage shouldBe false
    sourceStage.shuffleId shouldBe None
    sourceStage.inputSources should have length 1
    sourceStage.inputSources.head shouldBe a[StageBuilder.SourceInput]
    
    // Check shuffle stage (stage 1)
    val shuffleStage = stageGraph.stages(1)
    shuffleStage.id shouldBe 1
    shuffleStage.isShuffleStage shouldBe true
    shuffleStage.shuffleId shouldBe Some(0) // Uses source stage ID as shuffle ID
    shuffleStage.shuffleOperation shouldBe Some(groupByKeyPlan)
    shuffleStage.inputSources should have length 1
    shuffleStage.inputSources.head shouldBe a[StageBuilder.ShuffleInput]
    
    val shuffleInput = shuffleStage.inputSources.head.asInstanceOf[StageBuilder.ShuffleInput]
    shuffleInput.shuffleId shouldBe 0
    shuffleInput.numPartitions shouldBe 4
    
    // Check dependencies
    stageGraph.dependencies(1) should contain(0)
  }

  it should "create shuffle stages for reduceByKey operations" in {
    // Given: A reduceByKey operation plan
    val sourcePlan = Plan.Source(Seq(Partition(Seq(("a", 1), ("b", 2), ("a", 3)))))
    val reduceByKeyPlan = Plan.ReduceByKeyOp(sourcePlan, (x: Int, y: Int) => x + y)
    
    // When: We build the stage graph
    val stageGraph = StageBuilder.buildStageGraph(reduceByKeyPlan)
    
    // Then: We should get two stages with proper shuffle setup
    stageGraph.stages should have size 2
    stageGraph.finalStageId shouldBe 1
    
    val shuffleStage = stageGraph.stages(1)
    shuffleStage.isShuffleStage shouldBe true
    shuffleStage.shuffleOperation shouldBe Some(reduceByKeyPlan)
    shuffleStage.inputSources.head shouldBe a[StageBuilder.ShuffleInput]
  }

  it should "create shuffle stages for sortBy operations" in {
    // Given: A sortBy operation plan
    val sourcePlan = Plan.Source(Seq(Partition(Seq(3, 1, 2))))
    val sortByPlan = Plan.SortByOp(sourcePlan, (x: Int) => x, Ordering.Int)
    
    // When: We build the stage graph
    val stageGraph = StageBuilder.buildStageGraph(sortByPlan)
    
    // Then: We should get two stages with proper shuffle setup
    stageGraph.stages should have size 2
    stageGraph.finalStageId shouldBe 1
    
    val shuffleStage = stageGraph.stages(1)
    shuffleStage.isShuffleStage shouldBe true
    shuffleStage.shuffleOperation shouldBe Some(sortByPlan)
  }

  it should "handle narrow transformations after shuffle operations" in {
    // Given: A shuffle operation followed by narrow transformations
    val sourcePlan = Plan.Source(Seq(Partition(Seq(("a", 1), ("b", 2), ("a", 3)))))
    val groupByKeyPlan = Plan.GroupByKeyOp(sourcePlan)
    val mapPlan = Plan.MapOp(groupByKeyPlan, (x: (String, Iterable[Int])) => (x._1, x._2.sum))
    
    // When: We build the stage graph
    val stageGraph = StageBuilder.buildStageGraph(mapPlan)
    
    // Then: We should get three stages
    stageGraph.stages should have size 3
    stageGraph.finalStageId shouldBe 2
    
    // Source stage (0)
    val sourceStage = stageGraph.stages(0)
    sourceStage.isShuffleStage shouldBe false
    
    // Shuffle stage (1)
    val shuffleStage = stageGraph.stages(1)
    shuffleStage.isShuffleStage shouldBe true
    shuffleStage.shuffleOperation shouldBe Some(groupByKeyPlan)
    
    // Map stage after shuffle (2)
    val mapStage = stageGraph.stages(2)
    mapStage.isShuffleStage shouldBe false
    mapStage.inputSources should have length 1
    mapStage.inputSources.head shouldBe a[StageBuilder.ShuffleInput]
    
    // Check dependencies: map stage depends on shuffle stage, shuffle stage depends on source stage
    stageGraph.dependencies should have size 2
    stageGraph.dependencies(1) should contain(0) // shuffle depends on source
    stageGraph.dependencies(2) should contain(1) // map depends on shuffle
  }

  it should "chain narrow transformations in the same stage" in {
    // Given: Multiple narrow transformations
    val sourcePlan = Plan.Source(Seq(Partition(Seq(1, 2, 3, 4, 5))))
    val mapPlan = Plan.MapOp(sourcePlan, (x: Int) => x * 2)
    val filterPlan = Plan.FilterOp(mapPlan, (x: Int) => x > 5)
    val mapPlan2 = Plan.MapOp(filterPlan, (x: Int) => x.toString)
    
    // When: We build the stage graph
    val stageGraph = StageBuilder.buildStageGraph(mapPlan2)
    
    // Then: All narrow transformations should be chained in a single stage
    stageGraph.stages should have size 1
    stageGraph.dependencies should have size 0
    stageGraph.finalStageId shouldBe 0
    
    val stage = stageGraph.stages(0)
    stage.isShuffleStage shouldBe false
    stage.stage shouldBe a[Stage.ChainedStage[_, _, _]]
  }

  it should "handle union operations" in {
    // Given: A union operation
    val leftPlan = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val rightPlan = Plan.Source(Seq(Partition(Seq(4, 5, 6))))
    val unionPlan = Plan.UnionOp(leftPlan, rightPlan)
    
    // When: We build the stage graph
    val stageGraph = StageBuilder.buildStageGraph(unionPlan)
    
    // Then: We should get three stages - two source stages and one union stage
    stageGraph.stages should have size 3
    stageGraph.finalStageId shouldBe 2
    
    val unionStage = stageGraph.stages(2)
    unionStage.isShuffleStage shouldBe false
    unionStage.inputSources should have length 2 // Reads from both sources
    
    // Union stage should depend on both source stages
    stageGraph.dependencies(2) should contain(0)
    stageGraph.dependencies(2) should contain(1)
  }

  it should "reset stage IDs between different buildStageGraph calls" in {
    // Given: Two separate plans
    val plan1 = Plan.MapOp(Plan.Source(Seq(Partition(Seq(1, 2, 3)))), (x: Int) => x * 2)
    val plan2 = Plan.FilterOp(Plan.Source(Seq(Partition(Seq(4, 5, 6)))), (x: Int) => x > 4)
    
    // When: We build stage graphs for both plans
    val stageGraph1 = StageBuilder.buildStageGraph(plan1)
    val stageGraph2 = StageBuilder.buildStageGraph(plan2)
    
    // Then: Both should start with stage ID 0
    stageGraph1.finalStageId shouldBe 0
    stageGraph2.finalStageId shouldBe 0
    stageGraph1.stages.keys.min shouldBe 0
    stageGraph2.stages.keys.min shouldBe 0
  }

  "StageBuilder legacy buildStages" should "reject plans with shuffle operations" in {
    // Given: A plan with shuffle operations
    val sourcePlan = Plan.Source(Seq(Partition(Seq(("a", 1), ("b", 2)))))
    val groupByKeyPlan = Plan.GroupByKeyOp(sourcePlan)
    
    // When/Then: buildStages should throw an exception
    an[UnsupportedOperationException] should be thrownBy {
      StageBuilder.buildStages(groupByKeyPlan)
    }
  }

  it should "handle narrow-only plans" in {
    // Given: A plan with only narrow transformations
    val sourcePlan = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val mapPlan = Plan.MapOp(sourcePlan, (x: Int) => x * 2)
    val filterPlan = Plan.FilterOp(mapPlan, (x: Int) => x > 2)
    
    // When: We use the legacy buildStages method
    val stages = StageBuilder.buildStages(filterPlan)
    
    // Then: We should get a single stage with the source and chained operations
    stages should have length 1
    val (source, stage) = stages.head
    source shouldBe sourcePlan
    stage shouldBe a[Stage.ChainedStage[_, _, _]]
  }
}

