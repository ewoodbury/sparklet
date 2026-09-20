package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf, StageId}

// Hand-built graphs use existential StageInfo values the production types erase.
@SuppressWarnings(
  Array(
    "org.wartremover.warts.SeqApply",
    "org.wartremover.warts.Any",
  ),
)
class TestStageGraphValidation extends AnyFlatSpec with Matchers {

  private def stageInfo(
      id: Int,
      inputSources: Seq[StageBuilder.InputSource] = Seq.empty,
      isShuffleStage: Boolean = false,
      wideOp: Option[WideOp] = None,
      outputPartitioning: Option[StageBuilder.Partitioning] = None,
  ): StageBuilder.StageInfo =
    StageBuilder.StageInfo(
      id = StageId(id),
      stage = Stage.identity,
      inputSources = inputSources,
      isShuffleStage = isShuffleStage,
      wideOp = wideOp,
      outputPartitioning = outputPartitioning,
    )

  private def groupByWideOp(numPartitions: Int = 4): WideOp =
    GroupByKeyWideOp(
      SimpleWideOpMeta(kind = WideOpKind.GroupByKey, numPartitions = numPartitions),
    )

  private def graph(
      stages: Seq[StageBuilder.StageInfo],
      dependencies: Map[StageId, Set[StageId]] = Map.empty,
      finalStageId: StageId,
  ): StageBuilder.StageGraph =
    StageBuilder.StageGraph(stages.map(s => s.id -> s).toMap, dependencies, finalStageId)

  private def shuffleInput(
      stageId: Int,
      side: Option[StageBuilder.Side] = None,
      numPartitions: Int = 4,
  ): StageBuilder.InputSource =
    StageBuilder.ShuffleInput(StageId(stageId), side, numPartitions)

  "validateStageGraph" should "accept a single narrow stage" in {
    val g = graph(stages = Seq(stageInfo(0)), finalStageId = StageId(0))

    StageBuilder.validateStageGraph(g)
  }

  it should "accept a valid multi-stage graph with a shuffle boundary" in {
    val g = graph(
      stages = Seq(
        stageInfo(0),
        stageInfo(
          1,
          inputSources = Seq(shuffleInput(0)),
          isShuffleStage = true,
          wideOp = Some(groupByWideOp()),
          outputPartitioning = Some(StageBuilder.Partitioning(byKey = true, numPartitions = 4)),
        ),
      ),
      dependencies = Map(StageId(1) -> Set(StageId(0))),
      finalStageId = StageId(1),
    )

    StageBuilder.validateStageGraph(g)
  }

  it should "accept a valid join graph with proper side markers" in {
    val g = graph(
      stages = Seq(
        stageInfo(0),
        stageInfo(1),
        stageInfo(
          2,
          inputSources = Seq(
            shuffleInput(0, Some(StageBuilder.Side.Left)),
            shuffleInput(1, Some(StageBuilder.Side.Right)),
          ),
          isShuffleStage = true,
          wideOp = Some(JoinWideOp(SimpleWideOpMeta(kind = WideOpKind.Join, numPartitions = 4))),
        ),
      ),
      dependencies = Map(StageId(2) -> Set(StageId(0), StageId(1))),
      finalStageId = StageId(2),
    )

    StageBuilder.validateStageGraph(g)
  }

  it should "reject a missing final stage" in {
    val g = graph(stages = Seq(stageInfo(0)), finalStageId = StageId(7))

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject a dependency on a missing stage" in {
    val g = graph(
      stages = Seq(stageInfo(0), stageInfo(1, inputSources = Seq(shuffleInput(9)))),
      dependencies = Map(StageId(1) -> Set(StageId(9))),
      finalStageId = StageId(1),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject a stage present in dependencies but absent from the stages map" in {
    val g = graph(
      stages = Seq(stageInfo(0)),
      dependencies = Map(StageId(1) -> Set(StageId(0))),
      finalStageId = StageId(0),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject a self-dependency" in {
    val g = graph(
      stages = Seq(stageInfo(0)),
      dependencies = Map(StageId(0) -> Set(StageId(0))),
      finalStageId = StageId(0),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject a cycle" in {
    val g = graph(
      stages = Seq(stageInfo(0), stageInfo(1)),
      dependencies = Map(
        StageId(0) -> Set(StageId(1)),
        StageId(1) -> Set(StageId(0)),
      ),
      finalStageId = StageId(0),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject an orphaned stage unreachable from the final stage" in {
    val g = graph(
      stages = Seq(stageInfo(0), stageInfo(1)),
      dependencies = Map.empty,
      finalStageId = StageId(0),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject stage IDs that do not start from 0" in {
    val g = graph(
      stages = Seq(stageInfo(1), stageInfo(2)),
      dependencies = Map(StageId(2) -> Set(StageId(1))),
      finalStageId = StageId(2),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject a shuffle stage without wide operation metadata" in {
    val g = graph(
      stages = Seq(
        stageInfo(0),
        stageInfo(1, inputSources = Seq(shuffleInput(0)), isShuffleStage = true, wideOp = None),
      ),
      dependencies = Map(StageId(1) -> Set(StageId(0))),
      finalStageId = StageId(1),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject a multi-input shuffle stage missing side markers" in {
    val g = graph(
      stages = Seq(
        stageInfo(0),
        stageInfo(1),
        stageInfo(
          2,
          inputSources = Seq(shuffleInput(0, None), shuffleInput(1, None)),
          isShuffleStage = true,
          wideOp = Some(JoinWideOp(SimpleWideOpMeta(kind = WideOpKind.Join, numPartitions = 4))),
        ),
      ),
      dependencies = Map(StageId(2) -> Set(StageId(0), StageId(1))),
      finalStageId = StageId(2),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject a multi-input shuffle stage with duplicate side markers" in {
    val g = graph(
      stages = Seq(
        stageInfo(0),
        stageInfo(1),
        stageInfo(
          2,
          inputSources = Seq(
            shuffleInput(0, Some(StageBuilder.Side.Left)),
            shuffleInput(1, Some(StageBuilder.Side.Left)),
          ),
          isShuffleStage = true,
          wideOp = Some(JoinWideOp(SimpleWideOpMeta(kind = WideOpKind.Join, numPartitions = 4))),
        ),
      ),
      dependencies = Map(StageId(2) -> Set(StageId(0), StageId(1))),
      finalStageId = StageId(2),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject a multi-input shuffle stage with mismatched partition counts" in {
    val g = graph(
      stages = Seq(
        stageInfo(0),
        stageInfo(1),
        stageInfo(
          2,
          inputSources = Seq(
            shuffleInput(0, Some(StageBuilder.Side.Left), numPartitions = 4),
            shuffleInput(1, Some(StageBuilder.Side.Right), numPartitions = 8),
          ),
          isShuffleStage = true,
          wideOp = Some(JoinWideOp(SimpleWideOpMeta(kind = WideOpKind.Join, numPartitions = 4))),
        ),
      ),
      dependencies = Map(StageId(2) -> Set(StageId(0), StageId(1))),
      finalStageId = StageId(2),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject byKey partitioning with a non-positive partition count" in {
    val g = graph(
      stages = Seq(
        stageInfo(0, outputPartitioning =
          Some(StageBuilder.Partitioning(byKey = true, numPartitions = 0)),
        ),
      ),
      finalStageId = StageId(0),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "reject excessively large partition counts" in {
    val g = graph(
      stages = Seq(
        stageInfo(0, outputPartitioning =
          Some(StageBuilder.Partitioning(byKey = false, numPartitions = 2000000)),
        ),
      ),
      finalStageId = StageId(0),
    )

    an[IllegalStateException] should be thrownBy StageBuilder.validateStageGraph(g)
  }

  it should "validate graphs produced by real plans without error" in {
    SparkletConf.set(SparkletConf())
    try {
      val source: Plan[(String, Int)] =
        Plan.Source(Seq(Partition(Seq("a" -> 1, "b" -> 2, "a" -> 3))))
      val plan = Plan.GroupByKeyOp(Plan.MapOp(source, (x: (String, Int)) => x))
      val g = StageBuilder.buildStageGraph(plan)

      StageBuilder.validateStageGraph(g)
    } finally SparkletConf.set(SparkletConf())
  }
}
