package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf, StageId}
import com.ewoodbury.sparklet.execution.Operation

/**
 * Tests for the new Operation ADT and normalized InputSource modeling.
 */
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
end TestOperationsAndInputSources