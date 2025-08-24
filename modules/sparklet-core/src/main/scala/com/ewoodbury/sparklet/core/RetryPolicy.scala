package com.ewoodbury.sparklet.core

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Defines policies for retrying failed operations with exponential backoff and configurable
 * retryable exceptions.
 */
sealed trait RetryPolicy {

  /**
   * Determines if a failed operation should be retried based on attempt count and exception type
   */
  def shouldRetry(attempt: Int, exception: Throwable): Boolean

  /** Calculates the delay before the next retry attempt */
  def delayBeforeRetry(attempt: Int): FiniteDuration

  /** Maximum number of retry attempts allowed */
  def maxRetries: Int
}

object RetryPolicy {

  /**
   * Implements exponential backoff retry policy with configurable retryable exceptions.
   *
   * @param maxRetries
   *   Maximum number of retry attempts
   * @param baseDelay
   *   Base delay between retries
   * @param maxDelay
   *   Maximum delay cap to prevent excessive waits
   * @param retryableExceptions
   *   Set of exception classes that are considered retryable. If empty, all exceptions are
   *   considered retryable.
   */
  final case class ExponentialBackoff(
      maxRetries: Int,
      baseDelay: FiniteDuration,
      maxDelay: FiniteDuration,
      retryableExceptions: Set[Class[_]] = Set.empty,
  ) extends RetryPolicy {

    require(maxRetries >= 0, "maxRetries must be non-negative")
    require(baseDelay > Duration.Zero, "baseDelay must be positive")
    require(maxDelay >= baseDelay, "maxDelay must be >= baseDelay")

    override def shouldRetry(attempt: Int, exception: Throwable): Boolean =
      attempt <= maxRetries && isRetryable(exception)

    override def delayBeforeRetry(attempt: Int): FiniteDuration = {
      val exponentialDelay = baseDelay * math.pow(2, attempt - 1).toLong
      FiniteDuration(
        math.min(exponentialDelay.toMillis, maxDelay.toMillis),
        scala.concurrent.duration.MILLISECONDS,
      )
    }

    private def isRetryable(exception: Throwable): Boolean =
      retryableExceptions.isEmpty || retryableExceptions.exists(_.isInstance(exception))
  }

  /**
   * Creates a retry policy with no retries - operations fail immediately on first failure.
   */
  val NoRetry: RetryPolicy =
    ExponentialBackoff(0, FiniteDuration(1, "ms"), FiniteDuration(1, "ms"))

  /**
   * Creates a default retry policy with reasonable defaults for production use.
   */
  def default: RetryPolicy = ExponentialBackoff(
    maxRetries = 3,
    baseDelay = FiniteDuration(1, "second"),
    maxDelay = FiniteDuration(30, "seconds"),
  )

  /**
   * Creates a retry policy that only retries specific exception types.
   */
  def forExceptions(maxRetries: Int, baseDelay: FiniteDuration, maxDelay: FiniteDuration)(
      exceptions: Class[_]*,
  ): RetryPolicy =
    ExponentialBackoff(maxRetries, baseDelay, maxDelay, exceptions.toSet)
}
