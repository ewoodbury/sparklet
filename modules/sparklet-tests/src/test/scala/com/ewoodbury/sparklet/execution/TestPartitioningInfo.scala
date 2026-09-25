package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf, StageId}

/**
 * Row-path partitioning metadata: distribution, ordering, and layout.
 */
// Hand-built graphs use the erased identity stage, same as TestStageGraphValidation.
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TestPartitioningInfo extends AnyFlatSpec with Matchers:

  private val shuffleN: Int = SparkletConf.get.defaultShufflePartitions
  private val rowKey: Seq[Int] = PartitioningInfo.RowKeyOrdinals

  private def source(width: Int): Plan.Source[Int] =
    Plan.Source((0 until width).map(i => Partition(Seq(i))))

  private def pairs(width: Int): Plan[(Int, Int)] =
    Plan.MapOp(source(width), (x: Int) => (x % 3, x))

  private def finalStage[A](plan: Plan[A]): StageBuilder.StageInfo =
    val graph = StageBuilder.buildStageGraph(plan)
    graph.stages(graph.finalStageId)

  private def distributionOf[A](plan: Plan[A]): Distribution =
    finalStage(plan).outputPartitioning.get.distribution

  private def lone(info: PartitioningInfo): StageBuilder.StageGraph = {
    val stage = StageBuilder.StageInfo(
      id = StageId(0),
      stage = Stage.identity,
      inputSources = Seq.empty[StageBuilder.InputSource],
      isShuffleStage = false,
      wideOp = None,
      outputPartitioning = Some(info),
    )
    StageBuilder.StageGraph(Map(StageId(0) -> stage), Map.empty, StageId(0))
  }

  "sources" should "be an unknown distribution of boxed rows" in {
    val info = finalStage(source(3)).outputPartitioning.get

    info.distribution shouldBe Distribution.Unknown(3)
    info.ordering shouldBe OrderingTag.None
    info.layout shouldBe Layout.BoxedRows
  }

  it should "keep that unknown distribution through map" in {
    val plan = Plan.MapOp(source(4), (x: Int) => x + 1)

    finalStage(plan).outputPartitioning shouldBe Some(PartitioningInfo.unknown(4))
  }

  "keyed shuffles" should "be hash-partitioned on the row key" in {
    distributionOf(Plan.GroupByKeyOp(pairs(2))) shouldBe Distribution.Hash(shuffleN, rowKey)
    distributionOf(Plan.ReduceByKeyOp(pairs(2), (a: Int, b: Int) => a + b)) shouldBe
      Distribution.Hash(shuffleN, rowKey)
    distributionOf(Plan.PartitionByOp(pairs(2), 6)) shouldBe Distribution.Hash(6, rowKey)
    distributionOf(
      Plan.JoinOp(pairs(1), pairs(1), Some(Plan.JoinStrategy.ShuffleHash)),
    ) shouldBe Distribution.Hash(shuffleN, rowKey)
    distributionOf(Plan.CoGroupOp(pairs(1), pairs(1))) shouldBe Distribution.Hash(shuffleN, rowKey)

    val info = finalStage(Plan.GroupByKeyOp(pairs(2))).outputPartitioning.get
    info.ordering shouldBe OrderingTag.None
    info.layout shouldBe Layout.BoxedRows
  }

  "sortBy" should "be range-partitioned and sorted, with unknown direction" in {
    val plan = Plan.SortByOp(source(2), (x: Int) => x, Ordering.Int)
    val info = finalStage(plan).outputPartitioning.get

    info.distribution shouldBe Distribution.Range(shuffleN, rowKey)
    info.ordering shouldBe OrderingTag.Sorted(rowKey, Seq.empty[Boolean])
    info.layout shouldBe Layout.BoxedRows
  }

  it should "keep range and ordering through a following map" in {
    val plan = Plan.MapOp(
      Plan.SortByOp(source(2), (x: Int) => x, Ordering.Int),
      (x: Int) => x + 1,
    )

    finalStage(plan).outputPartitioning shouldBe Some(PartitioningInfo.rangeSorted(shuffleN))
  }

  "repartition and coalesce" should "use round-robin and unknown widths" in {
    finalStage(Plan.RepartitionOp(source(2), 5)).outputPartitioning shouldBe Some(
      PartitioningInfo.roundRobin(5),
    )
    finalStage(Plan.CoalesceOp(source(8), 3)).outputPartitioning shouldBe Some(
      PartitioningInfo.unknown(3),
    )
  }

  "narrow key ops" should "drop hash on keys and values and keep it on mapValues" in {
    val partitioned = Plan.PartitionByOp(pairs(1), 4)

    finalStage(Plan.KeysOp(partitioned)).outputPartitioning shouldBe Some(
      PartitioningInfo.unknown(4),
    )
    finalStage(Plan.ValuesOp(partitioned)).outputPartitioning shouldBe Some(
      PartitioningInfo.unknown(4),
    )
    finalStage(Plan.MapValuesOp(partitioned, (v: Int) => v + 1)).outputPartitioning shouldBe Some(
      PartitioningInfo.hash(4),
    )
  }

  "union" should "drop partitioning metadata" in {
    finalStage(Plan.UnionOp(source(1), source(2))).outputPartitioning shouldBe
      Option.empty[PartitioningInfo]
  }

  "shuffle bypass" should "keep hash when groupByKey follows a matching partitionBy" in {
    val plan = Plan.GroupByKeyOp(Plan.PartitionByOp(pairs(1), shuffleN))
    val stage = finalStage(plan)

    stage.isShuffleStage shouldBe false
    stage.outputPartitioning shouldBe Some(PartitioningInfo.hash(shuffleN))
  }

  it should "keep hash when reduceByKey follows a matching partitionBy" in {
    val plan = Plan.ReduceByKeyOp(
      Plan.PartitionByOp(pairs(1), shuffleN),
      (a: Int, b: Int) => a + b,
    )
    val stage = finalStage(plan)

    stage.isShuffleStage shouldBe false
    stage.outputPartitioning shouldBe Some(PartitioningInfo.hash(shuffleN))
  }

  it should "still shuffle groupByKey after a range sort" in {
    val plan = Plan.GroupByKeyOp(
      Plan.SortByOp(pairs(2), (kv: (Int, Int)) => kv._1, Ordering.Int),
    )
    val stage = finalStage(plan)

    stage.isShuffleStage shouldBe true
    stage.outputPartitioning shouldBe Some(PartitioningInfo.hash(shuffleN))
  }

  it should "skip repartition when the upstream width matches and is not hash" in {
    val sameWidth = Plan.RepartitionOp(source(4), 4)
    val afterSort = Plan.RepartitionOp(
      Plan.SortByOp(source(2), (x: Int) => x, Ordering.Int),
      shuffleN,
    )

    finalStage(sameWidth).isShuffleStage shouldBe false
    finalStage(sameWidth).outputPartitioning shouldBe Some(PartitioningInfo.roundRobin(4))
    finalStage(afterSort).isShuffleStage shouldBe false
    finalStage(afterSort).outputPartitioning shouldBe Some(PartitioningInfo.roundRobin(shuffleN))
  }

  it should "still shuffle repartition after a hash of the same width" in {
    val plan = Plan.RepartitionOp(Plan.PartitionByOp(pairs(1), 4), 4)
    val stage = finalStage(plan)

    stage.isShuffleStage shouldBe true
    stage.outputPartitioning shouldBe Some(PartitioningInfo.roundRobin(4))
  }

  "validation" should "accept singleton and a positive columnar batch" in {
    StageBuilder.validateStageGraph(
      lone(
        PartitioningInfo(
          distribution = Distribution.Singleton,
          ordering = OrderingTag.None,
          layout = Layout.Columnar(batchSize = 1024),
        ),
      ),
    )
    StageBuilder.validateStageGraph(lone(PartitioningInfo.unknown(0)))
  }

  it should "reject empty hash keys, non-positive range, and a non-positive batch" in {
    val rejected = Seq(
      PartitioningInfo(
        Distribution.Hash(4, Seq.empty),
        OrderingTag.None,
        Layout.BoxedRows,
      ),
      PartitioningInfo(
        Distribution.Range(0, rowKey),
        OrderingTag.None,
        Layout.BoxedRows,
      ),
      PartitioningInfo.roundRobin(0),
      PartitioningInfo(
        Distribution.Unknown(1),
        OrderingTag.Sorted(rowKey, Seq(true, false)),
        Layout.BoxedRows,
      ),
      PartitioningInfo(
        Distribution.Unknown(1),
        OrderingTag.None,
        Layout.Columnar(batchSize = 0),
      ),
    )

    rejected.foreach { info =>
      an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(lone(info))
    }
  }
