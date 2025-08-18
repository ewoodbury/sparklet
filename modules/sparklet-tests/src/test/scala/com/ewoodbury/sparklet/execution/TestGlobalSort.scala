package com.ewoodbury.sparklet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.api.DistCollection
import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf}
import com.ewoodbury.sparklet.runtime.SparkletRuntime

class TestGlobalSort extends AnyFlatSpec with Matchers {
  private val toDistCollection: [T] => (seq: Seq[T]) => DistCollection[T] = [T] => (seq: Seq[T]) =>
    DistCollection(Plan.Source(Seq(Partition(seq))))

  "Global sort planning" should "allocate a range-partitioned shuffle using default partitions" in {
    SparkletRuntime.get.shuffle.clear()
    val ds = toDistCollection(Seq(5, 3, 9, 1, 7)).sortBy(identity)

    val graph = StageBuilder.buildStageGraph(ds.plan)
    val sortStageInfoOpt = graph.stages.values.find(_.shuffleOperation.exists(_.isInstanceOf[Plan.SortByOp[_, _]]))
    sortStageInfoOpt.isDefined shouldBe true
    val sortStageInfo = sortStageInfoOpt.get

    sortStageInfo.isShuffleStage shouldBe true
    // The sort stage should read from a shuffle of the prior stage with configured partition count
    val expectedN = SparkletConf.get.defaultShufflePartitions
    val inputN: Seq[Int] = sortStageInfo.inputSources.collect { case StageBuilder.ShuffleFrom(_, n) => n }
    assert(inputN.nonEmpty)
    inputN.headOption shouldBe Some(expectedN)
  }

  it should "produce a globally sorted result on skewed datasets" in {
    SparkletRuntime.get.shuffle.clear()
    val skew = Seq.fill(500)(0) ++ (1 to 1000) ++ Seq.fill(300)(1000) ++ (200 to 400)
    val ds = toDistCollection(skew)
    val result = ds.sortBy(identity).collect().toSeq
    result shouldBe skew.sorted
  }

  it should "sort by a custom key across multiple partitions" in {
    SparkletRuntime.get.shuffle.clear()
    val data = Seq("aaaa", "b", "ccc", "dd", "e", "fffff", "gg")
    val ds = toDistCollection(data)
    val result = ds.sortBy(_.length).collect().toSeq
    result.map(_.length) shouldBe data.map(_.length).sorted
  }
}



