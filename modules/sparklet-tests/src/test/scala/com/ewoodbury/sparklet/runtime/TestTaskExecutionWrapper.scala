package com.ewoodbury.sparklet.runtime

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{Partition, RetryPolicy}
import com.ewoodbury.sparklet.runtime.api.RunnableTask

import scala.concurrent.duration._

/**
 * Demonstration test showing TaskExecutionWrapper functionality working
 */
class TestTaskExecutionWrapper extends AnyFlatSpec with Matchers {

  class FailingTask(var attempts: Int = 0, succeedAfter: Int = 2) extends RunnableTask[String, String]:
    override def run(): Partition[String] = {
      attempts += 1
      if (attempts >= succeedAfter) {
        Partition(Seq(s"Success on attempt $attempts"))
      } else {
        throw new RuntimeException(s"Failed on attempt $attempts")
      }
    }

  "TaskExecutionWrapper Retry Policy" should "successfully retry failed tasks" in {
    val failingTask = new FailingTask(succeedAfter = 3)
    val retryPolicy = RetryPolicy.ExponentialBackoff(
      maxRetries = 3,
      baseDelay = 10.millis,
      maxDelay = 100.millis
    )

    val wrapper = TaskExecutionWrapper.withRetryPolicy[IO](retryPolicy)

    val result = wrapper.executeWithRetry(failingTask).unsafeRunSync()

    // Should have succeeded after retries
    result.data should contain ("Success on attempt 3")
    failingTask.attempts shouldBe 3
  }

  it should "fail permanently when retries exhausted" in {
    val failingTask = new FailingTask(succeedAfter = 10) // Will never succeed
    val retryPolicy = RetryPolicy.ExponentialBackoff(
      maxRetries = 2,
      baseDelay = 1.millis,
      maxDelay = 10.millis
    )

    val wrapper = TaskExecutionWrapper.withRetryPolicy[IO](retryPolicy)

    // Should throw exception after retries exhausted
    val exception = intercept[RuntimeException] {
      wrapper.executeWithRetry(failingTask).unsafeRunSync()
    }

    exception.getMessage should include ("Failed on attempt 3")
    failingTask.attempts shouldBe 3 // Initial + 2 retries
  }

  it should "respect no-retry policy" in {
    val failingTask = new FailingTask(succeedAfter = 2)
    val wrapper = TaskExecutionWrapper.noRetries[IO]

    val exception = intercept[RuntimeException] {
      wrapper.executeWithRetry(failingTask).unsafeRunSync()
    }

    exception.getMessage should include ("Failed on attempt 1")
    failingTask.attempts shouldBe 1 // Only initial attempt, no retry
  }

  it should "handle successful tasks immediately" in {
    val successfulTask = new RunnableTask[String, String]:
      override def run(): Partition[String] = Partition(Seq("Immediate success"))

    val wrapper = TaskExecutionWrapper.default[IO]
    val result = wrapper.executeWithRetry(successfulTask).unsafeRunSync()

    result.data should contain ("Immediate success")
  }

  it should "implement exponential backoff correctly" in {
    val policy = RetryPolicy.ExponentialBackoff(
      maxRetries = 3,
      baseDelay = 100.millis,
      maxDelay = 1000.millis
    )

    policy.delayBeforeRetry(1) shouldBe 100.millis
    policy.delayBeforeRetry(2) shouldBe 200.millis
    policy.delayBeforeRetry(3) shouldBe 400.millis
  }

  it should "cap delays at maxDelay" in {
    val policy = RetryPolicy.ExponentialBackoff(
      maxRetries = 10,
      baseDelay = 100.millis,
      maxDelay = 500.millis
    )

    policy.delayBeforeRetry(1) shouldBe 100.millis
    policy.delayBeforeRetry(2) shouldBe 200.millis
    policy.delayBeforeRetry(3) shouldBe 400.millis
    policy.delayBeforeRetry(4) shouldBe 500.millis // capped
    policy.delayBeforeRetry(5) shouldBe 500.millis // still capped
  }
}
