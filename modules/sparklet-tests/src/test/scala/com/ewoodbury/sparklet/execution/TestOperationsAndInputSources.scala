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
end TestOperationsAndInputSources