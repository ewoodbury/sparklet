package com.ewoodbury.sparklet.core

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

/**
 * Centralized configuration for Sparklet runtime defaults.
 *
 * @param defaultShufflePartitions
 *   Number of partitions to use for shuffle outputs when not otherwise specified.
 * @param defaultParallelism
 *   Default task parallelism when deriving concurrency without explicit hints.
 * @param threadPoolSize
 *   Size of the thread pool backing the local TaskScheduler.
 * @param maxTaskRetries
 *   Maximum number of retries for failed tasks.
 * @param baseRetryDelayMs
 *   Base delay between retries in milliseconds (exponential backoff).
 * @param maxRetryDelayMs
 *   Maximum backoff delay to prevent excessive waits.
 */
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
final case class SparkletConf(
    defaultShufflePartitions: Int = 4,
    defaultParallelism: Int = 4,
    threadPoolSize: Int = 4,
    /** Approximate samples per logical partition to estimate sort key distribution. */
    sortSamplePerPartition: Int = 20,
    /** Cap on total number of samples to bound driver memory. */
    sortMaxSample: Int = 20000,
    /** Size threshold (in number of records) below which broadcast-hash join is preferred. */
    broadcastJoinThreshold: Long = 1000L,
    /**
     * Whether to enable sort-merge join for large datasets. If false, always uses shuffle-hash
     * join.
     */
    enableSortMergeJoin: Boolean = true,
    maxTaskRetries: Int = 3,
    baseRetryDelayMs: Long = 1000L,
    maxRetryDelayMs: Long = 30000L,
    /** Whether to enable lineage-based recovery on task failure */
    enableLineageRecovery: Boolean = true,
    /** Maximum time to wait for task completion before considering it failed */
    taskTimeoutMs: Long = 300000L, // 5 minutes
    /** Whether to enable speculative execution for slow tasks */
    enableSpeculativeExecution: Boolean = false,
    /** Slow task threshold for speculative execution (percentage of median) */
    speculativeExecutionThreshold: Double = 1.5,
):

  /**
   * Builds the task retry policy from this configuration.
   */
  def retryPolicy: RetryPolicy = RetryPolicy.ExponentialBackoff(
    maxRetries = maxTaskRetries,
    baseDelay = FiniteDuration(baseRetryDelayMs, MILLISECONDS),
    maxDelay = FiniteDuration(maxRetryDelayMs, MILLISECONDS),
  )

@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments", "org.wartremover.warts.Var"))
object SparkletConf {
  @volatile private var current: SparkletConf = SparkletConf()

  /** Returns the active configuration. */
  def get: SparkletConf = current

  /** Overrides the active configuration at runtime. */
  def set(conf: SparkletConf): Unit = {
    current = conf
  }
}
