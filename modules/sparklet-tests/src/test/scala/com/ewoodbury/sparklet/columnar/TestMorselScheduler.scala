package com.ewoodbury.sparklet.columnar

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestMorselScheduler extends AnyFlatSpec with Matchers:

  "scheduler" should "return results in morsel order on one thread" in {
    val morsels = (0 until 5).toVector.map(row => BatchCodec.ints(Seq(row)))

    val lengths = MorselScheduler.map(morsels, parallelism = 1)((_, batch) => batch.length)

    lengths shouldBe morsels.map(_.length)
  }

  it should "finish a heavy morsel while other workers take the rest" in {
    val heavy = BatchCodec.ints(Seq.fill(10_000)(1))
    val lights = (0 until 7).toVector.map(row => BatchCodec.ints(Seq(row)))
    val morsels = heavy +: lights
    val started = new CountDownLatch(1)

    val results = MorselScheduler.map(morsels, parallelism = 4) { (index, batch) =>
      val opened =
        if (index == 0) then started.await(5, TimeUnit.SECONDS)
        else {
          started.countDown()
          true
        }
      (opened, batch.length)
    }

    results.map(_._2) shouldBe morsels.map(_.length)
    results.lift(0).getOrElse(fail("missing heavy morsel"))._1 shouldBe true
  }

  it should "fail the call when a morsel throws" in {
    val morsels = Vector(BatchCodec.ints(Seq(1)), BatchCodec.ints(Seq(2)))

    val thrown = intercept[IllegalStateException] {
      MorselScheduler.map(morsels, parallelism = 2) { (index, _) =>
        if (index == 0) then throw new IllegalStateException("morsel failed")
        else index
      }
    }

    thrown.getMessage shouldBe "morsel failed"
  }
