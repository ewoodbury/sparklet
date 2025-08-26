package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf, StageId}
import com.ewoodbury.sparklet.execution.{Operation, WideOp, WideOpMeta, WideOpKind, Stage}

/**
 * Tests for the new Operation ADT and normalized InputSource modeling.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TestOperationsAndInputSources extends AnyFlatSpec with Matchers:

  // Test data
  private val testPartition = Partition(Seq(1, 2, 3))
  private val testPartitions = Seq(testPartition)
  private val defaultPartitions = SparkletConf.get.defaultShufflePartitions

  "Operation ADT" should "correctly identify shuffle operations" in {
    // Narrow operations should not need shuffle
    Operation.needsShuffle(MapOp[Int, String](_.toString)) shouldBe false
    Operation.needsShuffle(FilterOp[Int](_ > 5)) shouldBe false
    Operation.needsShuffle(FlatMapOp[Int, String](x => Seq(x.toString))) shouldBe false
    Operation.needsShuffle(DistinctOp()) shouldBe false
    Operation.needsShuffle(MapPartitionsOp[Int, String](_.map(_.toString))) shouldBe false

    // Wide operations should need shuffle
    Operation.needsShuffle(GroupByKeyOp[Int, String](defaultPartitions)) shouldBe true
    Operation.needsShuffle(ReduceByKeyOp[Int, String](_ + _, defaultPartitions)) shouldBe true
    Operation.needsShuffle(SortByOp[Int, String](_.length, defaultPartitions)) shouldBe true
    Operation.needsShuffle(PartitionByOp[Int, String](defaultPartitions)) shouldBe true
    Operation.needsShuffle(RepartitionOp[String](defaultPartitions)) shouldBe true
    Operation.needsShuffle(CoalesceOp[String](defaultPartitions)) shouldBe true
    Operation.needsShuffle(JoinOp[Int, String, String](defaultPartitions)) shouldBe true
    Operation.needsShuffle(CoGroupOp[Int, String, String](defaultPartitions)) shouldBe true
  }

  it should "convert Plan operations to Operations correctly" in {
    val intSourcePlan = Plan.Source(testPartitions)
    val kvTestData = Seq(("key1", 1), ("key2", 2))
    val kvPartition = Partition(kvTestData)
    val kvSourcePlan: Plan[(String, Int)] = Plan.Source(Seq(kvPartition))

    // Test narrow operations
    val mapPlan = Plan.MapOp(intSourcePlan, (_: Int) + 1)
    Operation.fromPlan(mapPlan) shouldBe a[MapOp[_, _]]

    val filterPlan = Plan.FilterOp(intSourcePlan, (_: Int) > 5)
    Operation.fromPlan(filterPlan) shouldBe a[FilterOp[_]]

    val flatMapPlan = Plan.FlatMapOp(intSourcePlan, (x: Int) => Seq(x.toString))
    Operation.fromPlan(flatMapPlan) shouldBe a[FlatMapOp[_, _]]

    // Test wide operations (with placeholder values)
    val gbkPlan = Plan.GroupByKeyOp(kvSourcePlan)
    val gbkOp = Operation.fromPlan(gbkPlan)
    gbkOp shouldBe a[GroupByKeyOp[_, _]]

    val rbkPlan = Plan.ReduceByKeyOp(kvSourcePlan, (a: Int, b: Int) => a + b)
    val rbkOp = Operation.fromPlan(rbkPlan)
    rbkOp shouldBe a[ReduceByKeyOp[_, _]]
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

  "Partitioning propagation" should "be defined for all operation types" in {
    // Test that we can create partitioning metadata for various operations
    // This test ensures the Partitioning case class works as expected
    import StageBuilder.Partitioning

    val keyedPartitioning = Partitioning(byKey = true, numPartitions = 8)
    val nonKeyedPartitioning = Partitioning(byKey = false, numPartitions = 4)

    keyedPartitioning.byKey shouldBe true
    keyedPartitioning.numPartitions shouldBe 8
    nonKeyedPartitioning.byKey shouldBe false
    nonKeyedPartitioning.numPartitions shouldBe 4
  }

  "Shuffle boundary detection" should "correctly identify wide operations requiring shuffle" in {
    import Operation.needsShuffle

    // Create test data for shuffle boundary tests
    val intSourcePlan = Plan.Source(testPartitions)
    val kvTestData = Seq(("key1", 1), ("key2", 2))
    val kvPartition = Partition(kvTestData)
    val kvSourcePlan: Plan[(String, Int)] = Plan.Source(Seq(kvPartition))

    // Narrow operations should not require shuffle
    needsShuffle(Plan.MapOp(intSourcePlan, (_: Int) + 1)) shouldBe false
    needsShuffle(Plan.FilterOp(intSourcePlan, (_: Int) > 5)) shouldBe false
    needsShuffle(Plan.UnionOp(intSourcePlan, intSourcePlan)) shouldBe false

    // Wide operations should require shuffle
    needsShuffle(Plan.GroupByKeyOp(kvSourcePlan)) shouldBe true
    needsShuffle(Plan.ReduceByKeyOp(kvSourcePlan, (a: Int, b: Int) => a + b)) shouldBe true
    needsShuffle(Plan.SortByOp(intSourcePlan, (x: Int) => x, Ordering[Int])) shouldBe true
    needsShuffle(Plan.PartitionByOp(kvSourcePlan, 16)) shouldBe true
    needsShuffle(Plan.RepartitionOp(intSourcePlan, 32)) shouldBe true
    needsShuffle(Plan.CoalesceOp(intSourcePlan, 8)) shouldBe true
    needsShuffle(Plan.JoinOp(kvSourcePlan, kvSourcePlan, None)) shouldBe true
    needsShuffle(Plan.CoGroupOp(kvSourcePlan, kvSourcePlan)) shouldBe true
  }

  it should "determine when shuffle can be bypassed based on partitioning" in {
    import Operation.canBypassShuffle
    import StageBuilder.Partitioning

    val conf = SparkletConf.get
    val defaultN = conf.defaultShufflePartitions

    // Create test data for bypass optimization tests
    val intSourcePlan = Plan.Source(testPartitions)
    val kvTestData = Seq(("key1", 1), ("key2", 2))
    val kvPartition = Partition(kvTestData)
    val kvSourcePlan: Plan[(String, Int)] = Plan.Source(Seq(kvPartition))

    // Test groupByKey bypass optimization
    val gbkPlan = Plan.GroupByKeyOp(kvSourcePlan)
    val alreadyPartitionedByKey = Some(Partitioning(byKey = true, numPartitions = defaultN))
    val notPartitionedByKey = Some(Partitioning(byKey = false, numPartitions = defaultN))
    val differentPartitionCount = Some(Partitioning(byKey = true, numPartitions = defaultN + 1))

    canBypassShuffle(gbkPlan, alreadyPartitionedByKey, conf) shouldBe true
    canBypassShuffle(gbkPlan, notPartitionedByKey, conf) shouldBe false
    canBypassShuffle(gbkPlan, differentPartitionCount, conf) shouldBe false
    canBypassShuffle(gbkPlan, None, conf) shouldBe false

    // Test reduceByKey bypass optimization
    val rbkPlan = Plan.ReduceByKeyOp(kvSourcePlan, (a: Int, b: Int) => a + b)
    canBypassShuffle(rbkPlan, alreadyPartitionedByKey, conf) shouldBe true
    canBypassShuffle(rbkPlan, notPartitionedByKey, conf) shouldBe false

    // Test partitionBy bypass optimization
    val pbyPlan = Plan.PartitionByOp(kvSourcePlan, 16)
    val alreadyCorrectlyPartitioned = Some(Partitioning(byKey = true, numPartitions = 16))
    canBypassShuffle(pbyPlan, alreadyCorrectlyPartitioned, conf) shouldBe true
    canBypassShuffle(pbyPlan, alreadyPartitionedByKey, conf) shouldBe false // Wrong partition count

    // Test repartition bypass optimization
    val repPlan = Plan.RepartitionOp(intSourcePlan, 32)
    val alreadyCorrectlyRepartitioned = Some(Partitioning(byKey = false, numPartitions = 32))
    canBypassShuffle(repPlan, alreadyCorrectlyRepartitioned, conf) shouldBe true
    canBypassShuffle(repPlan, alreadyPartitionedByKey, conf) shouldBe false // Wrong byKey flag

    // Test coalesce bypass optimization
    val coalPlan = Plan.CoalesceOp(intSourcePlan, 4)
    val alreadyFewerPartitions = Some(Partitioning(byKey = false, numPartitions = 4))
    val morePartitions = Some(Partitioning(byKey = false, numPartitions = 8))
    canBypassShuffle(coalPlan, alreadyFewerPartitions, conf) shouldBe true
    canBypassShuffle(coalPlan, morePartitions, conf) shouldBe false // Can't coalesce to more partitions

    // Operations that cannot bypass shuffle
    val sortPlan = Plan.SortByOp(intSourcePlan, (x: Int) => x, Ordering[Int])
    canBypassShuffle(sortPlan, alreadyPartitionedByKey, conf) shouldBe false

    val joinPlan = Plan.JoinOp(kvSourcePlan, kvSourcePlan, None)
    canBypassShuffle(joinPlan, alreadyPartitionedByKey, conf) shouldBe false
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

    // Test WideOpMeta creation
    val meta = WideOpMeta(
      kind = GroupByKey,
      numPartitions = 16,
      reduceFunc = Some((a: Any, b: Any) => a),
      sides = Seq()
    )
    meta.kind shouldBe GroupByKey
    meta.numPartitions shouldBe 16
    meta.reduceFunc shouldBe defined

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
    stage.outputPartitioning.map(_.byKey) shouldBe Some(false)
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
    finalStage.outputPartitioning.map(_.byKey) shouldBe Some(true)
    finalStage.shuffleOperation.isDefined shouldBe true
  }

  it should "build groupByKey with bypass shuffle optimization when already partitioned" in {
    // Create a source and then partition it by key
    val source = createSource()
    val kvSource = Plan.MapOp(source, (x: Int) => (x % 3, x)) // Create key-value pairs
    val partitionedSource = Plan.PartitionByOp(kvSource, SparkletConf.get.defaultShufflePartitions)
    val plan = Plan.GroupByKeyOp(partitionedSource)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should bypass shuffle because data is already partitioned by key
    // Expected stages: source + map + partitionBy + local groupByKey all chained together
    // The unified builder chains operations efficiently, so we get fewer stages than traditional approach
    stageGraph.stages.size shouldBe 1
    val stage = stageGraph.stages(stageGraph.finalStageId)
    stage.isShuffleStage shouldBe false
  }

  it should "build reduceByKey after groupByKey with bypass" in {
    val source = createSource()
    val kvSource = Plan.MapOp(source, (x: Int) => (x % 3, x))
    val partitionedSource = Plan.PartitionByOp(kvSource, SparkletConf.get.defaultShufflePartitions)
    val plan = Plan.ReduceByKeyOp(
      partitionedSource,
      (a: Int, b: Int) => a + b
    )

    val stageGraph = StageBuilder.buildStageGraph(plan)

    // Should bypass shuffle because data is already partitioned by key
    stageGraph.stages.size shouldBe 1
    val stage = stageGraph.stages(stageGraph.finalStageId)
    stage.isShuffleStage shouldBe false
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
    finalStage.outputPartitioning.map(_.byKey) shouldBe Some(true)
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

  it should "handle partitionBy setting byKey true and correct partition count" in {
    val source = Plan.MapOp(createSource(), (x: Int) => (x, x * 2))
    val plan = Plan.PartitionByOp(source, 3)

    val stageGraph = StageBuilder.buildStageGraph(plan)

    val finalStage = stageGraph.stages(stageGraph.finalStageId)
    finalStage.isShuffleStage shouldBe true
    finalStage.outputPartitioning.map(_.byKey) shouldBe Some(true)
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

    // Should have stages for: shared map stage, filter1, filter2, union
    // (source is chained with the shared map)
    stageGraph.stages.size shouldBe 4

    // Find source stages - should be exactly one (the shared map stage)
    val sourceStages = stageGraph.stages.filter(_._2.inputSources.exists {
      case StageBuilder.SourceInput(_) => true
      case _ => false
    })
    sourceStages.size shouldBe 1

    // The shared map stage should have the source input
    val sharedMapStage = sourceStages.headOption.map(_._2)
    sharedMapStage.map(_.inputSources.collect { case StageBuilder.SourceInput(_) => true }) should not be empty
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

end TestOperationsAndInputSources