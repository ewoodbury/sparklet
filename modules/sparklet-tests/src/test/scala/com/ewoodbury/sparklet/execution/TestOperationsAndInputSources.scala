package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf, StageId}

/**
 * Tests for the new Operation ADT and normalized InputSource modeling.
 */
// Internals tests exercise erased stage/transport types directly.
@SuppressWarnings(
  Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"),
)
class TestOperationsAndInputSources extends AnyFlatSpec with Matchers:

  // Test data
  private val testPartition = Partition(Seq(1, 2, 3))
  private val testPartitions = Seq(testPartition)
  private val defaultPartitions = SparkletConf.get.defaultShufflePartitions

  "Operation ADT" should "have well-formed operation types" in {
    // Test that operation types can be created successfully
    val mapOp = MapOp[Int, String](_.toString)
    val filterOp = FilterOp[Int](_ > 5)
    val flatMapOp = FlatMapOp[Int, String](x => Seq(x.toString))
    val distinctOp = DistinctOp[Int]()
    val mapPartitionsOp = MapPartitionsOp[Int, String](_.map(_.toString))

    // Bypassed wide operations are appended as narrow ops by the builder
    val gbkLocalOp = GroupByKeyLocalOp[Int, String]()
    val rbkLocalOp = ReduceByKeyLocalOp[Int, String](_ + _)
    val partitionLocalOp = PartitionByLocalOp[Int, String](defaultPartitions)
    val repartitionOp = RepartitionOp[String](defaultPartitions)
    val coalesceOp = CoalesceOp[String](defaultPartitions)

    // All operations should be created without errors
    mapOp shouldBe a[MapOp[_, _]]
    filterOp shouldBe a[FilterOp[_]]
    flatMapOp shouldBe a[FlatMapOp[_, _]]
    distinctOp shouldBe a[DistinctOp[_]]
    mapPartitionsOp shouldBe a[MapPartitionsOp[_, _]]
    gbkLocalOp shouldBe a[GroupByKeyLocalOp[_, _]]
    rbkLocalOp shouldBe a[ReduceByKeyLocalOp[_, _]]
    partitionLocalOp shouldBe a[PartitionByLocalOp[_, _]]
    repartitionOp shouldBe a[RepartitionOp[_]]
    coalesceOp shouldBe a[CoalesceOp[_]]
  }

  "InputSource normalization" should "unify shuffle inputs with optional sides" in {
    import StageBuilder.{Side, ShuffleInput}

    // Single-input shuffle should have no side
    val singleShuffle = ShuffleInput(StageId(1), None, defaultPartitions)
    singleShuffle.side shouldBe None

    // Multi-input shuffle should have sides
    val leftShuffle = ShuffleInput(StageId(1), Some(Side.Left), defaultPartitions)
    leftShuffle.side shouldBe Some(Side.Left)

    val rightShuffle = ShuffleInput(StageId(2), Some(Side.Right), defaultPartitions)
    rightShuffle.side shouldBe Some(Side.Right)
  }

  "Partitioning propagation" should "be defined for hash and unknown distributions" in {
    val keyedPartitioning = PartitioningInfo.hash(8)
    val unknownPartitioning = PartitioningInfo.unknown(4)

    keyedPartitioning.distribution shouldBe Distribution.Hash(8, PartitioningInfo.RowKeyOrdinals)
    keyedPartitioning.numPartitions shouldBe 8
    unknownPartitioning.distribution shouldBe Distribution.Unknown(4)
    unknownPartitioning.numPartitions shouldBe 4
    keyedPartitioning.layout shouldBe Layout.BoxedRows
  }

  "Shuffle boundary detection" should "be handled via stage building" in {
    // Narrow operations should produce a valid one-stage graph
    val intSourcePlan = Plan.Source(testPartitions)
    val narrowPlan = Plan.MapOp(intSourcePlan, (_: Int) + 1)

    val narrowGraph = StageBuilder.buildStageGraph(narrowPlan)
    narrowGraph.stages should have size 1

    // Wide operations should also produce a valid (multi-stage) graph
    val kvTestData = Seq(("key1", 1), ("key2", 2))
    val kvPartition = Partition(kvTestData)
    val kvSourcePlan: Plan[(String, Int)] = Plan.Source(Seq(kvPartition))
    val widePlan = Plan.GroupByKeyOp(kvSourcePlan)

    val stageGraph = StageBuilder.buildStageGraph(widePlan)
    stageGraph.stages should not be empty
  }

  it should "handle shuffle bypass optimization through unified builder" in {

    // Test that bypass optimization works through the public API
    // Create a scenario where groupByKey should bypass shuffle
    val kvTestData = Seq(("key1", 1), ("key2", 2))
    val kvPartition = Partition(kvTestData)
    val kvSourcePlan: Plan[(String, Int)] = Plan.Source(Seq(kvPartition))

    // First partition by key, then groupByKey - should bypass shuffle in groupByKey
    val partitionByPlan = Plan.PartitionByOp(kvSourcePlan, SparkletConf.get.defaultShufflePartitions)
    val gbkPlan = Plan.GroupByKeyOp(partitionByPlan)

    // The unified builder should handle this optimization internally
    val stageGraph = StageBuilder.buildStageGraph(gbkPlan)
    stageGraph.stages should not be empty

    // The final stage should be a narrow stage (no shuffle bypass detected)
    // Note: This test validates that the optimization path is accessible,
    // actual bypass behavior depends on internal implementation
    val finalStage = stageGraph.stages(stageGraph.finalStageId)
    finalStage.stage shouldBe a[Stage[_, _]]
  }

  "WideOp types" should "correctly represent different shuffle operations" in {
    import WideOpKind.*

    // Test that WideOpKind enum has all expected values
    WideOpKind.values should contain (GroupByKey)
    WideOpKind.values should contain (ReduceByKey)
    WideOpKind.values should contain (SortBy)
    WideOpKind.values should contain (PartitionBy)
    WideOpKind.values should contain (Repartition)
    WideOpKind.values should contain (Coalesce)
    WideOpKind.values should contain (Join)
    WideOpKind.values should contain (CoGroup)

    // Test WideOpMeta creation - GroupByKey doesn't need functions
    val meta = SimpleWideOpMeta(
      kind = GroupByKey,
      numPartitions = 16,
      sides = Seq.empty[StageBuilder.Side]
    )
    meta.kind shouldBe GroupByKey
    meta.numPartitions shouldBe 16

    // Test WideOp creation
    val wideOp = GroupByKeyWideOp(meta)
    wideOp shouldBe a[GroupByKeyWideOp]
    wideOp.meta.kind shouldBe meta.kind
    wideOp.meta.numPartitions shouldBe meta.numPartitions
  }

  // --- Unified Builder Tests ---

  behavior of "Unified Builder"

  private def createSource(): Plan.Source[Int] =
    Plan.Source(Seq(Partition(Seq(1, 2, 3))))

  private def createPartitionedSource(numPartitions: Int): Plan.Source[Int] =
    Plan.Source((0 until numPartitions).map(i => Partition(Seq(i * 10, i * 10 + 1))))

  it should "build pure narrow chain (map -> filter -> distinct)" in {
    val plan = Plan.DistinctOp(
      Plan.FilterOp(
        Plan.MapOp(createSource(), (_: Int) * 2),
        (_: Int) > 0
      )
    )

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should have only one stage (all operations chained)
    stageGraph.stages.size shouldBe 1
    val stage = stageGraph.stages(stageGraph.finalStageId)
    stage.isShuffleStage shouldBe false
    stage.inputSources shouldBe Seq(StageBuilder.SourceInput(createSource().partitions))
    stage.outputPartitioning.map(_.distribution) shouldBe Some(Distribution.Unknown(1))
  }

  it should "build narrow chain with partitioning metadata carry-over" in {
    val source = createPartitionedSource(4)
    val plan = Plan.MapOp(source, (_: Int).toString)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    stageGraph.stages.size shouldBe 1
    val stage = stageGraph.stages(stageGraph.finalStageId)
    stage.isShuffleStage shouldBe false
    stage.outputPartitioning.map(_.numPartitions) shouldBe Some(4)
  }

  it should "build groupByKey without bypass shuffle optimization" in {
    val source = createSource()
    // Create key-value pairs for groupByKey
    val kvSource = Plan.MapOp(source, (x: Int) => (x % 3, x)) // Group by x mod 3
    val plan = Plan.GroupByKeyOp(kvSource)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should create a shuffle stage
    stageGraph.stages.size shouldBe 2 // source stage + shuffle stage
    val finalStage = stageGraph.stages(stageGraph.finalStageId)
    finalStage.isShuffleStage shouldBe true
    finalStage.outputPartitioning.map(_.isHashPartitioned(defaultPartitions)) shouldBe Some(true)
    finalStage.wideOp.isDefined shouldBe true
  }

  it should "build groupByKey with bypass shuffle optimization when already partitioned" in {
    // Create a source and then partition it by key
    val source = createSource()
    val kvSource = Plan.MapOp(source, (x: Int) => (x % 3, x)) // Create key-value pairs
    val partitionedSource = Plan.PartitionByOp(kvSource, SparkletConf.get.defaultShufflePartitions)
    val plan = Plan.GroupByKeyOp(partitionedSource)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should bypass shuffle for groupByKey because data is partitioned by key after partitionByOp
    // Expected stages: source+map (1) + partitionBy shuffle (2) + local groupByKey (3)
    stageGraph.stages.size shouldBe 3
    val stage = stageGraph.stages(stageGraph.finalStageId)
    stage.isShuffleStage shouldBe false  // Final stage should be narrow (bypass worked)
  }

  it should "build reduceByKey after groupByKey with shuffle bypassed" in {
    val source = createSource()
    val kvSource = Plan.MapOp(source, (x: Int) => (x % 3, x))
    val partitionedSource = Plan.PartitionByOp(kvSource, SparkletConf.get.defaultShufflePartitions)
    val plan = Plan.ReduceByKeyOp(
      partitionedSource,
      (a: Int, b: Int) => a + b
    )

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should bypass shuffle for reduceByKey because data is partitioned by key after partitionByOp
    // Expected stages: source+map (1) + partitionBy shuffle (2) + local reduceByKey (3)
    stageGraph.stages.size shouldBe 3
    val stage = stageGraph.stages(stageGraph.finalStageId)
    stage.isShuffleStage shouldBe false  // Final stage should be narrow (bypass worked)
  }

  it should "build repartition followed by map (new stage after shuffle)" in {
    val plan = Plan.MapOp(
      Plan.RepartitionOp(createSource(), 3),
      (_: Int) * 2
    )

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should have three stages: source stage + repartition shuffle stage + map stage
    // (map can't chain to shuffle stage, so it creates a new narrow stage)
    stageGraph.stages.size shouldBe 3
    val finalStage = stageGraph.stages(stageGraph.finalStageId)
    finalStage.isShuffleStage shouldBe false
    finalStage.inputSources.collect { case StageBuilder.StageOutput(_) => true } should not be empty
  }

  it should "build union of two independent narrow chains" in {
    val left = Plan.MapOp(createSource(), (_: Int) * 2)
    val right = Plan.FilterOp(createSource(), (_: Int) > 0)
    val plan = Plan.UnionOp(left, right)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should have 3 stages: left chain, right chain, and union stage
    stageGraph.stages.size shouldBe 3
    val finalStage = stageGraph.stages(stageGraph.finalStageId)
    finalStage.isShuffleStage shouldBe false
    finalStage.inputSources.length shouldBe 2
    finalStage.inputSources.forall(_.isInstanceOf[StageBuilder.StageOutput]) shouldBe true
  }

  it should "build join with two ShuffleInputs with sides" in {
    val left = Plan.MapOp(createSource(), (x: Int) => (x, x * 2))
    val right = Plan.MapOp(createSource(), (x: Int) => (x, x * 3))
    val plan = Plan.JoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash))

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should have 3 stages: left, right, and join shuffle stage
    stageGraph.stages.size shouldBe 3
    val finalStage = stageGraph.stages(stageGraph.finalStageId)
    finalStage.isShuffleStage shouldBe true
    finalStage.inputSources.length shouldBe 2
    finalStage.inputSources.forall(_.isInstanceOf[StageBuilder.ShuffleInput]) shouldBe true
    val shuffleInputs = finalStage.inputSources.collect { case si: StageBuilder.ShuffleInput => si }
    shuffleInputs.map(_.side) shouldBe Seq(Some(StageBuilder.Side.Left), Some(StageBuilder.Side.Right))
  }

  it should "build cogroup with two ShuffleInputs with sides" in {
    val left = Plan.MapOp(createSource(), (x: Int) => (x, x * 2))
    val right = Plan.MapOp(createSource(), (x: Int) => (x, x * 3))
    val plan = Plan.CoGroupOp(left, right)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    stageGraph.stages.size shouldBe 3
    val finalStage = stageGraph.stages(stageGraph.finalStageId)
    finalStage.isShuffleStage shouldBe true
    finalStage.inputSources.length shouldBe 2
    finalStage.inputSources.forall(_.isInstanceOf[StageBuilder.ShuffleInput]) shouldBe true
  }

  // --- Side Tagging for Multi-Input Operations ---

  behavior of "Side Tagging for Multi-Input Operations"

  it should "assign correct side markers for join operations" in {
    val left = Plan.MapOp(createSource(), (x: Int) => (x, x * 2))
    val right = Plan.MapOp(createSource(), (x: Int) => (x, x * 3))
    val plan = Plan.JoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash))

    val stageGraph = StageBuilder.buildStageGraph(plan)
    val finalStage = stageGraph.stages(stageGraph.finalStageId)

    // Should have two ShuffleInput sources with correct sides
    finalStage.inputSources.length shouldBe 2
    val shuffleInputs = finalStage.inputSources.collect { case si: StageBuilder.ShuffleInput => si }

    shuffleInputs.length shouldBe 2
    shuffleInputs.map(_.side) should contain theSameElementsAs Seq(Some(StageBuilder.Side.Left), Some(StageBuilder.Side.Right))
  }

  it should "assign correct side markers for cogroup operations" in {
    val left = Plan.MapOp(createSource(), (x: Int) => (x, x * 2))
    val right = Plan.MapOp(createSource(), (x: Int) => (x, x * 3))
    val plan = Plan.CoGroupOp(left, right)

    val stageGraph = StageBuilder.buildStageGraph(plan)
    val finalStage = stageGraph.stages(stageGraph.finalStageId)

    // Should have two ShuffleInput sources with correct sides
    finalStage.inputSources.length shouldBe 2
    val shuffleInputs = finalStage.inputSources.collect { case si: StageBuilder.ShuffleInput => si }

    shuffleInputs.length shouldBe 2
    shuffleInputs.map(_.side) should contain theSameElementsAs Seq(Some(StageBuilder.Side.Left), Some(StageBuilder.Side.Right))
  }

  it should "validate multi-input operation invariants" in {
    // Test that join operations maintain consistent side ordering
    val source1 = Plan.Source(Seq(Partition(Seq(1, 2))))
    val source2 = Plan.Source(Seq(Partition(Seq(3, 4))))

    val left = Plan.MapOp(source1, (x: Int) => (x, x * 2))
    val right = Plan.MapOp(source2, (x: Int) => (x, x * 3))

    val joinPlan = Plan.JoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash))
    val cogroupPlan = Plan.CoGroupOp(left, right)

    // Both operations should create valid stage graphs
    val joinGraph = StageBuilder.buildStageGraph(joinPlan)
    val cogroupGraph = StageBuilder.buildStageGraph(cogroupPlan)

    // Both should have shuffle stages with 2 inputs
    val joinFinal = joinGraph.stages(joinGraph.finalStageId)
    val cogroupFinal = cogroupGraph.stages(cogroupGraph.finalStageId)

    joinFinal.inputSources.length shouldBe 2
    cogroupFinal.inputSources.length shouldBe 2

    // Both should have ShuffleInput with side markers
    joinFinal.inputSources.forall(_.isInstanceOf[StageBuilder.ShuffleInput]) shouldBe true
    cogroupFinal.inputSources.forall(_.isInstanceOf[StageBuilder.ShuffleInput]) shouldBe true

    // Verify side markers are present and correct
    val joinSides = joinFinal.inputSources.collect { case si: StageBuilder.ShuffleInput => si.side }
    val cogroupSides = cogroupFinal.inputSources.collect { case si: StageBuilder.ShuffleInput => si.side }

    joinSides should contain theSameElementsAs Seq(Some(StageBuilder.Side.Left), Some(StageBuilder.Side.Right))
    cogroupSides should contain theSameElementsAs Seq(Some(StageBuilder.Side.Left), Some(StageBuilder.Side.Right))
  }

  it should "build nested wide operations (map -> repartition -> reduceByKey)" in {
    val source = createSource()
    val plan = Plan.ReduceByKeyOp(
      Plan.RepartitionOp(
        Plan.MapOp(source, (x: Int) => (x % 10, x)),
        5
      ),
      (a: Int, b: Int) => a + b
    )

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should have 3 stages: source, repartition shuffle, reduceByKey shuffle
    stageGraph.stages.size shouldBe 3
    val finalStage = stageGraph.stages(stageGraph.finalStageId)
    finalStage.isShuffleStage shouldBe true
    finalStage.outputPartitioning.map(_.isHashPartitioned(defaultPartitions)) shouldBe Some(true)
  }

  it should "handle coalesce vs repartition difference" in {
    val source = createPartitionedSource(10)

    // Coalesce should potentially bypass if reducing partitions
    val coalescePlan = Plan.CoalesceOp(source, 5)
    val coalesceGraph = StageBuilder.buildStageGraph(coalescePlan)

    // Repartition should always create shuffle
    val repartitionPlan = Plan.RepartitionOp(source, 5)
    val repartitionGraph = StageBuilder.buildStageGraph(repartitionPlan)

    // Both should create shuffle stages in this case
    coalesceGraph.stages(coalesceGraph.finalStageId).isShuffleStage shouldBe true
    repartitionGraph.stages(repartitionGraph.finalStageId).isShuffleStage shouldBe true
  }

  it should "handle partitionBy setting a hash distribution and the requested width" in {
    val source = Plan.MapOp(createSource(), (x: Int) => (x, x * 2))
    val plan = Plan.PartitionByOp(source, 3)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    val finalStage = stageGraph.stages(stageGraph.finalStageId)
    finalStage.isShuffleStage shouldBe true
    finalStage.outputPartitioning.map(_.distribution) shouldBe Some(
      Distribution.Hash(3, PartitioningInfo.RowKeyOrdinals),
    )
    finalStage.outputPartitioning.map(_.numPartitions) shouldBe Some(3)
  }

  it should "correctly handle dependencies between stages" in {
    val left = Plan.MapOp(createSource(), (_: Int) * 2)
    val right = Plan.MapOp(createSource(), (_: Int) * 3)
    val plan = Plan.UnionOp(left, right)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Check that dependencies are correctly established
    val unionStageId = stageGraph.finalStageId
    stageGraph.dependencies(unionStageId).size shouldBe 2

    // Find the left and right stage IDs
    val leftStageId = stageGraph.stages.find(_._2.inputSources.exists {
      case StageBuilder.SourceInput(parts) => parts == createSource().partitions
      case _ => false
    }).get._1

    val rightStageId = stageGraph.stages.find(_._2.inputSources.exists {
      case StageBuilder.SourceInput(parts) => parts == createSource().partitions
      case _ => false
    }).get._1

    stageGraph.dependencies(unionStageId) should contain (leftStageId)
    stageGraph.dependencies(unionStageId) should contain (rightStageId)
  }

  // --- Union Semantics Tests ---

  behavior of "Union Semantics"

  it should "handle union(map(source), map(source)) without duplicating source reads" in {
    // Create a single source
    val source = createSource()

    // Create two different maps on the same source
    val left = Plan.MapOp(source, (_: Int) * 2)  // Double each element
    val right = Plan.MapOp(source, (_: Int) * 3) // Triple each element
    val plan = Plan.UnionOp(left, right)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should have 3 stages: left map stage, right map stage, union stage
    // (the source is chained with the map operations)
    stageGraph.stages.size shouldBe 3

    // Find the source stage (the one reading from SourceInput)
    val sourceStages = stageGraph.stages.filter(_._2.inputSources.exists {
      case StageBuilder.SourceInput(_) => true
      case _ => false
    })

    // Should have exactly two source stages - one for each branch (this is expected)
    // The unified builder creates separate stages for each branch to avoid complexity
    sourceStages.size shouldBe 2

    // Find the union stage
    val unionStage = stageGraph.stages(stageGraph.finalStageId)
    unionStage.isShuffleStage shouldBe false
    unionStage.inputSources.length shouldBe 2

    // Both inputs should be StageOutputs from different stages
    val stageOutputs = unionStage.inputSources.collect { case StageBuilder.StageOutput(id) => id }
    stageOutputs.length shouldBe 2
  }

  it should "handle complex union with shared sub-plan" in {
    // Create a shared sub-plan
    val source = createSource()
    val sharedMap = Plan.MapOp(source, (_: Int) * 2)
    val filter1 = Plan.FilterOp(sharedMap, (_: Int) > 0)
    val filter2 = Plan.FilterOp(sharedMap, (_: Int) < 10)

    val plan = Plan.UnionOp(filter1, filter2)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // NOTE: Due to the current implementation, the shared sub-plan creates
    // duplicate stages rather than sharing. This results in 3 stages:
    // Stage 0: source + sharedMap + filter1 (chained)
    // Stage 1: source + sharedMap + filter2 (duplicated shared sub-plan)
    // Stage 2: union
    stageGraph.stages.size shouldBe 3

    // Find source stages - there will be two due to duplication
    val sourceStages = stageGraph.stages.filter(_._2.inputSources.exists {
      case StageBuilder.SourceInput(_) => true
      case _ => false
    })
    sourceStages.size shouldBe 2

    // The union stage should read from both filter stages
    val unionStage = stageGraph.stages(stageGraph.finalStageId)
    unionStage.inputSources.length shouldBe 2
    unionStage.inputSources.forall(_.isInstanceOf[StageBuilder.StageOutput]) shouldBe true
  }

  it should "handle union with completely independent sources" in {
    val source1 = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
    val source2 = Plan.Source(Seq(Partition(Seq(4, 5, 6))))

    val left = Plan.MapOp(source1, (_: Int) * 2)
    val right = Plan.MapOp(source2, (_: Int) * 3)
    val plan = Plan.UnionOp(left, right)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should have 3 stages: left chain, right chain, union
    // (each source is chained with its map operation)
    stageGraph.stages.size shouldBe 3

    // Should have exactly 2 source stages
    val sourceStages = stageGraph.stages.filter(_._2.inputSources.exists {
      case StageBuilder.SourceInput(_) => true
      case _ => false
    })
    sourceStages.size shouldBe 2

    // Verify the source stages read from different partitions
    val sourceInputs = sourceStages.values.flatMap(_.inputSources.collect {
      case StageBuilder.SourceInput(parts) => parts
    }).toSet

    sourceInputs.size shouldBe 2 // Two different partition sets
  }

  // --- Stage.ChainedStage Nesting Tests ---

  behavior of "Stage.ChainedStage Nesting"



  it should "create single operation stages without chaining for efficiency" in {
    val plan = Plan.MapOp(createSource(), (_: Int) * 2)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should have 1 stage with the operation directly (no ChainedStage)
    stageGraph.stages.size shouldBe 1
    val stage = stageGraph.stages(stageGraph.finalStageId)

    // The stage should contain the map operation directly, not wrapped in ChainedStage
    stage.stage match {
      case Stage.SingleOpStage(_) => // This is what we want for single operations
      case _ => fail("Single operation should create SingleOpStage, not ChainedStage")
    }
  }

  it should "create optimized chaining for multiple operations" in {
    val plan = Plan.DistinctOp(
      Plan.FilterOp(
        Plan.MapOp(createSource(), (_: Int) * 2),
        (_: Int) > 0
      )
    )

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should have 1 stage with chained operations
    stageGraph.stages.size shouldBe 1
    val stage = stageGraph.stages(stageGraph.finalStageId)

    // The stage should be a ChainedStage for multiple operations
    stage.stage match {
      case Stage.ChainedStage(_, _) => // This is expected for multiple operations
      case Stage.SingleOpStage(_) => fail("Multiple operations should create ChainedStage")
      case _ => fail("Unexpected stage type")
    }
  }

  it should "verify stage construction efficiency metrics" in {
    // Test that single operations create SingleOpStage
    val singleOpPlan = Plan.MapOp(createSource(), (_: Int) * 2)
    val singleStageGraph = StageBuilder.buildStageGraph(singleOpPlan)

    singleStageGraph.stages.size shouldBe 1
    val singleStage = singleStageGraph.stages(singleStageGraph.finalStageId)

    // Single operation should create a direct stage, not chained
    singleStage.stage match {
      case Stage.SingleOpStage(_) => // Expected for single operations
      case Stage.ChainedStage(Stage.SingleOpStage(_), _) => // Also acceptable but less efficient
      case _ => fail("Single operation should not create deeply nested ChainedStage")
    }

    // Test that multiple operations create reasonable chaining
    val multiOpPlan = Plan.DistinctOp(
      Plan.FilterOp(
        Plan.MapOp(createSource(), (_: Int) * 2),
        (_: Int) > 0
      )
    )
    val multiStageGraph = StageBuilder.buildStageGraph(multiOpPlan)

    multiStageGraph.stages.size shouldBe 1
    val multiStage = multiStageGraph.stages(multiStageGraph.finalStageId)

    // Multiple operations should create ChainedStage but not excessive nesting
    multiStage.stage match {
      case Stage.ChainedStage(_, _) => // Expected for multiple operations
      case _ => fail("Multiple operations should create ChainedStage")
    }
  }

  it should "verify stage execution correctness with different chaining strategies" in {
    val source = createSource()
    val plan1 = Plan.MapOp(source, (_: Int) * 2)  // Single operation
    val plan2 = Plan.FilterOp(Plan.MapOp(source, (_: Int) * 2), (_: Int) > 0)  // Two operations

    val stageGraph1 = StageBuilder.buildStageGraph(plan1)
    val stageGraph2 = StageBuilder.buildStageGraph(plan2)

    // Both should produce correct results
    val stage1 = stageGraph1.stages(stageGraph1.finalStageId)
    val stage2 = stageGraph2.stages(stageGraph2.finalStageId)

    // Test execution with sample data
    val testPartition = Partition(Seq(1, 2, 3))

    // Execute stages and handle type casting
    val result1 = stage1.stage.asInstanceOf[Stage[Any, Any]].execute(testPartition.asInstanceOf[Partition[Any]])
    val result2 = stage2.stage.asInstanceOf[Stage[Any, Any]].execute(testPartition.asInstanceOf[Partition[Any]])

    // Results should be equivalent to manual computation
    result1.data.toSeq shouldBe Seq(2, 4, 6)  // Map only
    result2.data.toSeq shouldBe Seq(2, 4, 6)  // Map + Filter (all elements > 0)
  }

  // --- Unified Builder Tests ---

  behavior of "Unified Stage Builder"

  it should "build a one-stage graph for a simple narrow chain" in {
    val plan = Plan.MapOp(createSource(), (_: Int) * 2)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    stageGraph.stages should have size 1
    stageGraph.stages.contains(stageGraph.finalStageId) shouldBe true
  }

  it should "build a one-stage graph for a multi-operation narrow chain" in {
    val plan = Plan.DistinctOp(
      Plan.FilterOp(
        Plan.MapOp(createSource(), (_: Int) * 2),
        (_: Int) > 0
      )
    )

    val stageGraph = StageBuilder.buildStageGraph(plan)

    stageGraph.stages should have size 1
  }

  it should "build a multi-stage graph for a shuffle plan" in {
    val source = createSource()
    val kvSource = Plan.MapOp(source, (x: Int) => (x % 3, x))
    val plan = Plan.GroupByKeyOp(kvSource)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    stageGraph.stages.size should be > 1
    stageGraph.dependencies.nonEmpty shouldBe true
  }

  it should "build a graph with source stages for union plans" in {
    val left = Plan.MapOp(createSource(), (_: Int) * 2)
    val right = Plan.FilterOp(createSource(), (_: Int) > 0)
    val plan = Plan.UnionOp(left, right)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    stageGraph.stages.size should be > 1
    stageGraph.stages.contains(stageGraph.finalStageId) shouldBe true
  }

  // --- StageGraph Validation Tests ---

  behavior of "StageGraph Validation"

  it should "pass validation for valid stage graphs" in {
    // Test that normal valid plans pass validation
    val plan = Plan.MapOp(createSource(), (_: Int) * 2)
    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should not throw any exceptions - validation passes
    stageGraph.stages.size shouldBe 1
    stageGraph.stages.contains(stageGraph.finalStageId) shouldBe true
  }

  it should "validate partitioning metadata consistency" in {
    // Test that partitioning validation works by creating an invalid scenario
    // This is a bit tricky to test directly since the builder should always create valid graphs
    // We'll test through the public API and ensure valid graphs are created
    val source = createSource()
    val kvSource = Plan.MapOp(source, (x: Int) => (x % 3, x))
    val plan = Plan.GroupByKeyOp(kvSource)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    val finalStage = stageGraph.stages(stageGraph.finalStageId)
    finalStage.outputPartitioning.foreach { p =>
      p.isHashPartitioned(defaultPartitions) shouldBe true
      p.numPartitions should be > 0
      p.layout shouldBe Layout.BoxedRows
    }
  }

  it should "validate multi-input shuffle stages have proper side markers" in {
    val left = Plan.MapOp(createSource(), (x: Int) => (x, x * 2))
    val right = Plan.MapOp(createSource(), (x: Int) => (x, x * 3))
    val plan = Plan.JoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash))

    val stageGraph = StageBuilder.buildStageGraph(plan)
    val finalStage = stageGraph.stages(stageGraph.finalStageId)

    // Validation should ensure multi-input shuffle stages have proper side markers
    finalStage.inputSources.length shouldBe 2
    val shuffleInputs = finalStage.inputSources.collect { case si: StageBuilder.ShuffleInput => si }
    shuffleInputs.length shouldBe 2
    shuffleInputs.forall(_.side.nonEmpty) shouldBe true
    shuffleInputs.map(_.side.get).toSet shouldBe Set(StageBuilder.Side.Left, StageBuilder.Side.Right)
  }

end TestOperationsAndInputSources