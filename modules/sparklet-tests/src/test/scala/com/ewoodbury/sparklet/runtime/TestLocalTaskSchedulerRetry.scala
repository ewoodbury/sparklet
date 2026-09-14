package com.ewoodbury.sparklet.runtime

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.FiniteDuration

import cats.effect.unsafe.implicits.global
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{Partition, RetryPolicy, SparkletConf}
import com.ewoodbury.sparklet.runtime.api.RunnableTask
import com.ewoodbury.sparklet.runtime.local.LocalTaskScheduler

class TestLocalTaskSchedulerRetry extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val fastConf = SparkletConf(
    maxTaskRetries = 2,
    baseRetryDelayMs = 1L,
    maxRetryDelayMs = 2L,
  )

  override def beforeEach(): Unit = SparkletConf.set(fastConf)

  override def afterEach(): Unit = SparkletConf.set(SparkletConf())

  private def flakyTask(
      failuresBeforeSuccess: Int,
      attempts: AtomicInteger,
  ): RunnableTask[Int, Int] = new RunnableTask[Int, Int] {
    def run(): Partition[Int] =
      if (attempts.incrementAndGet() <= failuresBeforeSuccess) {
        throw new IllegalStateException("transient failure")
      } else {
        Partition(Seq(42))
      }
  }

  private def alwaysFailingTask(attempts: AtomicInteger): RunnableTask[Int, Int] =
    new RunnableTask[Int, Int] {
      def run(): Partition[Int] = {
        attempts.incrementAndGet()
        throw new IllegalStateException("permanent failure")
      }
    }

  "LocalTaskScheduler.submit" should "retry transient failures per the configured policy" in {
    val scheduler = new LocalTaskScheduler(parallelism = 2)
    val attempts = new AtomicInteger(0)

    val result = scheduler.submit(Seq(flakyTask(failuresBeforeSuccess = 2, attempts))).unsafeRunSync()

    result.head.data.toSeq shouldEqual Seq(42)
    attempts.get shouldEqual 3
  }

  it should "fail permanently after exhausting the configured retry budget" in {
    val scheduler = new LocalTaskScheduler(parallelism = 2)
    val attempts = new AtomicInteger(0)

    an[IllegalStateException] should be thrownBy scheduler
      .submit(Seq(alwaysFailingTask(attempts)))
      .unsafeRunSync()

    attempts.get shouldEqual 3
  }

  "LocalTaskScheduler.submitWithRetry" should "allow an explicit policy override" in {
    val scheduler = new LocalTaskScheduler(parallelism = 2)
    val attempts = new AtomicInteger(0)
    // The task needs 4 runs to succeed; the configured budget (2 retries) would be
    // insufficient, so only the explicit policy of 3 retries can complete it.
    val explicitPolicy = RetryPolicy.ExponentialBackoff(
      maxRetries = 3,
      baseDelay = FiniteDuration(1, "ms"),
      maxDelay = FiniteDuration(2, "ms"),
    )

    val result =
      scheduler.submitWithRetry(Seq(flakyTask(failuresBeforeSuccess = 3, attempts)), explicitPolicy).unsafeRunSync()

    result.head.data.toSeq shouldEqual Seq(42)
    attempts.get shouldEqual 4
  }

  it should "preserve input task order in results" in {
    val scheduler = new LocalTaskScheduler(parallelism = 4)
    val tasks = Seq(1, 2, 3).map { value =>
      new RunnableTask[Int, Int] {
        def run(): Partition[Int] = Partition(Seq(value))
      }
    }

    val result = scheduler.submit(tasks).unsafeRunSync()

    result.map(partition => partition.data.head).toSeq shouldEqual Seq(1, 2, 3)
  }
}
