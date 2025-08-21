package com.ewoodbury.sparklet.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class TestRetryPolicy extends AnyFlatSpec with Matchers {

  "ExponentialBackoff" should "not retry when maxRetries is 0" in {
    val policy = RetryPolicy.ExponentialBackoff(0, 1.second, 10.seconds)

    policy.shouldRetry(1, new RuntimeException("test")) shouldBe false
    policy.maxRetries shouldBe 0
  }

  it should "retry within maxRetries limit" in {
    val policy = RetryPolicy.ExponentialBackoff(3, 1.second, 10.seconds)

    policy.shouldRetry(1, new RuntimeException("test")) shouldBe true
    policy.shouldRetry(2, new RuntimeException("test")) shouldBe true
    policy.shouldRetry(3, new RuntimeException("test")) shouldBe true
    policy.shouldRetry(4, new RuntimeException("test")) shouldBe false
  }

  it should "implement exponential backoff correctly" in {
    val policy = RetryPolicy.ExponentialBackoff(4, 1000.millis, 10000.millis)

    policy.delayBeforeRetry(1) shouldBe 1000.millis
    policy.delayBeforeRetry(2) shouldBe 2000.millis
    policy.delayBeforeRetry(3) shouldBe 4000.millis
    policy.delayBeforeRetry(4) shouldBe 8000.millis
  }

  it should "cap delay at maxDelay" in {
    val policy = RetryPolicy.ExponentialBackoff(10, 1000.millis, 5000.millis)

    policy.delayBeforeRetry(1) shouldBe 1000.millis
    policy.delayBeforeRetry(2) shouldBe 2000.millis
    policy.delayBeforeRetry(3) shouldBe 4000.millis
    policy.delayBeforeRetry(4) shouldBe 5000.millis // capped
    policy.delayBeforeRetry(5) shouldBe 5000.millis // still capped
  }

  it should "validate constructor parameters" in {
    // Valid parameters should work
    val validPolicy = RetryPolicy.ExponentialBackoff(5, 1.second, 30.seconds)
    validPolicy.maxRetries shouldBe 5

    // Invalid parameters should throw exceptions
    intercept[IllegalArgumentException] {
      RetryPolicy.ExponentialBackoff(-1, 1.second, 30.seconds)
    }

    intercept[IllegalArgumentException] {
      RetryPolicy.ExponentialBackoff(5, Duration.Zero, 30.seconds)
    }

    intercept[IllegalArgumentException] {
      RetryPolicy.ExponentialBackoff(5, 10.seconds, 1.second) // maxDelay < baseDelay
    }
  }

  it should "filter retryable exceptions when configured" in {
    val policy = RetryPolicy.ExponentialBackoff(
      maxRetries = 3,
      baseDelay = 1.second,
      maxDelay = 10.seconds,
      retryableExceptions = Set(classOf[IllegalArgumentException])
    )

    // Should retry IllegalArgumentException
    policy.shouldRetry(1, new IllegalArgumentException("test")) shouldBe true

    // Should not retry RuntimeException
    policy.shouldRetry(1, new RuntimeException("test")) shouldBe false

    // Should not retry subclasses of non-retryable exceptions
    policy.shouldRetry(1, new Exception("test")) shouldBe false
  }

  it should "retry all exceptions when no specific exceptions configured" in {
    val policy = RetryPolicy.ExponentialBackoff(3, 1.second, 10.seconds)

    policy.shouldRetry(1, new RuntimeException("test")) shouldBe true
    policy.shouldRetry(1, new IllegalArgumentException("test")) shouldBe true
    policy.shouldRetry(1, new Exception("test")) shouldBe true
  }

  it should "handle inheritance correctly for exception filtering" in {
    val policy = RetryPolicy.ExponentialBackoff(
      maxRetries = 3,
      baseDelay = 1.second,
      maxDelay = 10.seconds,
      retryableExceptions = Set(classOf[RuntimeException])
    )

    // Should retry RuntimeException
    policy.shouldRetry(1, new RuntimeException("test")) shouldBe true

    // Should retry subclasses of RuntimeException
    policy.shouldRetry(1, new IllegalArgumentException("test")) shouldBe true

    // Should not retry other exceptions
    policy.shouldRetry(1, new Exception("test")) shouldBe false
  }

  "RetryPolicy.NoRetry" should "never retry any exception" in {
    val policy = RetryPolicy.NoRetry

    policy.shouldRetry(1, new RuntimeException("test")) shouldBe false
    policy.shouldRetry(1, new Exception("test")) shouldBe false
    policy.maxRetries shouldBe 0
  }

  "RetryPolicy.default" should "provide sensible defaults" in {
    val policy = RetryPolicy.default

    policy.maxRetries shouldBe 3
    policy.delayBeforeRetry(1) shouldBe 1.second
    policy.shouldRetry(1, new RuntimeException("test")) shouldBe true
    policy.shouldRetry(4, new RuntimeException("test")) shouldBe false
  }

  "RetryPolicy.forExceptions" should "create policy with specific exceptions" in {
    val policy = RetryPolicy.forExceptions(2, 500.millis, 5.seconds)(
      classOf[IllegalArgumentException],
      classOf[IllegalStateException]
    )

    policy.maxRetries shouldBe 2
    policy.delayBeforeRetry(1) shouldBe 500.millis

    // Should retry specified exceptions
    policy.shouldRetry(1, new IllegalArgumentException("test")) shouldBe true
    policy.shouldRetry(1, new IllegalStateException("test")) shouldBe true

    // Should not retry other exceptions
    policy.shouldRetry(1, new RuntimeException("test")) shouldBe false
  }

  it should "handle edge cases in delay calculation" in {
    val policy = RetryPolicy.ExponentialBackoff(5, 1.millis, 1000.millis)

    // Very small delays
    policy.delayBeforeRetry(1).toMillis shouldBe 1

    // Test potential overflow scenarios
    val largePolicy = RetryPolicy.ExponentialBackoff(50, 1.millis, Long.MaxValue.nanos)
    val delay = largePolicy.delayBeforeRetry(40)
    delay should be > Duration.Zero
    delay should be <= largePolicy.maxDelay
  }
}
