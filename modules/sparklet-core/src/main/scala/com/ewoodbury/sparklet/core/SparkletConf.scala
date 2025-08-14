package com.ewoodbury.sparklet.core

/**
 * Centralized configuration for Sparklet runtime defaults.
 */
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
final case class SparkletConf(
    /** Number of partitions to use for shuffle outputs when not otherwise specified. */
    defaultShufflePartitions: Int = 4,
    /** Default task parallelism when deriving concurrency without explicit hints. */
    defaultParallelism: Int = 4,
    /** Size of the thread pool backing the local TaskScheduler. */
    threadPoolSize: Int = 4,
    /** Approximate samples per logical partition to estimate sort key distribution. */
    sortSamplePerPartition: Int = 20,
    /** Cap on total number of samples to bound driver memory. */
    sortMaxSample: Int = 20000,
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
