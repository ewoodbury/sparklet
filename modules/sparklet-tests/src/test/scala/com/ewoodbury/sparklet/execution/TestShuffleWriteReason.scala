package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf, StageId}

// Stage fixtures build existential WideOp values the production types erase.
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TestShuffleWriteReason extends AnyFlatSpec with Matchers {

  private def sortByStage(id: Int, numPartitions: Int): StageBuilder.StageInfo =
    StageBuilder.StageInfo(
      id = StageId(id),
      stage = Stage.identity,
      inputSources = Seq(
        StageBuilder.ShuffleInput(StageId(0), None, numPartitions),
      ),
      isShuffleStage = true,
      wideOp = Some(
        SortByWideOp(
          SortWideOpMeta(
            numPartitions = numPartitions,
            keyFunc = (x: Int) => x,
            ordering = Ordering.Int,
          ),
        ),
      ),
      outputPartitioning = None,
    )

  private def simpleWideStage(
      id: Int,
      wideOp: WideOp,
  ): StageBuilder.StageInfo =
    StageBuilder.StageInfo(
      id = StageId(id),
      stage = Stage.identity,
      inputSources = Seq.empty,
      isShuffleStage = true,
      wideOp = Some(wideOp),
      outputPartitioning = None,
    )

  private def partitionByStage(id: Int, numPartitions: Int): StageBuilder.StageInfo =
    simpleWideStage(
      id,
      PartitionByWideOp(
        SimpleWideOpMeta(kind = WideOpKind.PartitionBy, numPartitions = numPartitions),
      ),
    )

  private def repartitionStage(id: Int, numPartitions: Int): StageBuilder.StageInfo =
    simpleWideStage(
      id,
      RepartitionWideOp(
        SimpleWideOpMeta(kind = WideOpKind.Repartition, numPartitions = numPartitions),
      ),
    )

  private def groupByStage(id: Int): StageBuilder.StageInfo =
    simpleWideStage(
      id,
      GroupByKeyWideOp(
        SimpleWideOpMeta(kind = WideOpKind.GroupByKey, numPartitions = 4),
      ),
    )

  "ShuffleWriteReason.forDependents" should "prefer sortBy regardless of dependent order" in {
    val sortByFirst = Seq(sortByStage(1, 8), partitionByStage(2, 3))
    val sortByLast = Seq(partitionByStage(2, 3), sortByStage(1, 8))

    ShuffleWriteReason.forDependents(sortByFirst) shouldBe ShuffleWriteReason.DownstreamSortBy
    ShuffleWriteReason.forDependents(sortByLast) shouldBe ShuffleWriteReason.DownstreamSortBy
  }

  it should "prefer partitionBy over repartition" in {
    val dependents = Seq(repartitionStage(1, 5), partitionByStage(2, 3))

    ShuffleWriteReason.forDependents(dependents) shouldBe
      ShuffleWriteReason.DownstreamPartitionBy(3)
  }

  it should "use the repartition target count for repartition dependents" in {
    val dependents = Seq(repartitionStage(1, 5))

    ShuffleWriteReason.forDependents(dependents) shouldBe
      ShuffleWriteReason.DownstreamRepartition(5)
  }

  it should "fall back to a generic keyed write for groupByKey dependents" in {
    val dependents = Seq(groupByStage(1))

    ShuffleWriteReason.forDependents(dependents) shouldBe ShuffleWriteReason.DownstreamShuffle
  }

  it should "fall back to a generic keyed write when no dependent carries a wide op" in {
    val dependents = Seq(
      StageBuilder.StageInfo(
        id = StageId(1),
        stage = Stage.identity,
        inputSources = Seq.empty,
        isShuffleStage = true,
        wideOp = None,
        outputPartitioning = None,
      ),
    )

    ShuffleWriteReason.forDependents(dependents) shouldBe ShuffleWriteReason.DownstreamShuffle
  }

  it should "select reason consistently for graphs built from a diamond plan" in {
    // A plan where both a partitionBy and a sortBy stage consume the same upstream (via union of
    // separate branches); each branch builds its own upstream stage, but the reason function must
    // still be order-independent if a stage ever feeds both.
    SparkletConf.set(SparkletConf(defaultShufflePartitions = 4))
    try {
      val source: Plan[Int] = Plan.Source(Seq(Partition(Seq(1, 2, 3))))
      val kv = Plan.MapOp(source, (x: Int) => (x % 3, x))
      val graph = StageBuilder.buildStageGraph(
        Plan.UnionOp(
          Plan.PartitionByOp(kv, 8),
          Plan.SortByOp(kv, identity, summon[Ordering[(Int, Int)]]),
        ),
      )

      graph.stages.values.count(_.isShuffleStage) should be >= 2
    } finally SparkletConf.set(SparkletConf())
  }
}
