package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.StageId

@SuppressWarnings(Array("org.wartremover.warts.SeqApply"))
class TestTopologicalSort extends AnyFlatSpec with Matchers {

  "TopologicalSort.sort" should "include stages without dependency edges" in {
    val stages = Set(StageId(0), StageId(1), StageId(2))

    val order = TopologicalSort.sort(stages, Map.empty)

    order.toSet shouldBe stages
  }

  it should "order parent stages before children" in {
    val stages = Set(StageId(0), StageId(1))
    // Stage 1 depends on stage 0
    val dependencies = Map(StageId(1) -> Set(StageId(0)))

    val order = TopologicalSort.sort(stages, dependencies)

    order.indexOf(StageId(0)) should be < order.indexOf(StageId(1))
  }

  it should "handle multi-parent graphs (join/union shapes)" in {
    val stages = Set(StageId(0), StageId(1), StageId(2))
    // Stage 2 depends on both 0 and 1
    val dependencies = Map(StageId(2) -> Set(StageId(0), StageId(1)))

    val order = TopologicalSort.sort(stages, dependencies)

    order.indexOf(StageId(0)) should be < order.indexOf(StageId(2))
    order.indexOf(StageId(1)) should be < order.indexOf(StageId(2))
  }

  it should "handle multi-shuffle chains" in {
    val stages = Set(StageId(0), StageId(1), StageId(2))
    val dependencies = Map(
      StageId(1) -> Set(StageId(0)),
      StageId(2) -> Set(StageId(1)),
    )

    val order = TopologicalSort.sort(stages, dependencies)

    order shouldBe List(StageId(0), StageId(1), StageId(2))
  }

  it should "throw on cyclic dependencies" in {
    val stages = Set(StageId(0), StageId(1))
    val dependencies = Map(
      StageId(0) -> Set(StageId(1)),
      StageId(1) -> Set(StageId(0)),
    )

    an[IllegalStateException] should be thrownBy TopologicalSort.sort(stages, dependencies)
  }
}
