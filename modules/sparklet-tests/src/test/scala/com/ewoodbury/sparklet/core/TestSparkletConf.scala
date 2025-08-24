package com.ewoodbury.sparklet.core

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestSparkletConf extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val originalConf = SparkletConf.get

  override def afterEach(): Unit = {
    SparkletConf.set(originalConf)
    ()
  }

  "SparkletConf" should "provide default fault tolerance configuration values" in {
    val conf = SparkletConf()

    conf.maxTaskRetries shouldBe 3
    conf.baseRetryDelayMs shouldBe 1000L
    conf.maxRetryDelayMs shouldBe 30000L
    conf.enableLineageRecovery shouldBe true
    conf.taskTimeoutMs shouldBe 300000L
    conf.enableSpeculativeExecution shouldBe false
    conf.speculativeExecutionThreshold shouldBe 1.5
  }

  it should "allow overriding fault tolerance configuration" in {
    val customConf = SparkletConf().copy(
      maxTaskRetries = 5,
      baseRetryDelayMs = 2000L,
      maxRetryDelayMs = 60000L,
      enableLineageRecovery = false,
      taskTimeoutMs = 600000L,
      enableSpeculativeExecution = true,
      speculativeExecutionThreshold = 2.0
    )

    SparkletConf.set(customConf)
    val retrieved = SparkletConf.get

    retrieved.maxTaskRetries shouldBe 5
    retrieved.baseRetryDelayMs shouldBe 2000L
    retrieved.maxRetryDelayMs shouldBe 60000L
    retrieved.enableLineageRecovery shouldBe false
    retrieved.taskTimeoutMs shouldBe 600000L
    retrieved.enableSpeculativeExecution shouldBe true
    retrieved.speculativeExecutionThreshold shouldBe 2.0
  }

  it should "preserve existing configuration when setting new values" in {
    val customConf = SparkletConf().copy(
      defaultShufflePartitions = 8,
      maxTaskRetries = 2
    )

    SparkletConf.set(customConf)
    val retrieved = SparkletConf.get

    retrieved.defaultShufflePartitions shouldBe 8
    retrieved.maxTaskRetries shouldBe 2
    retrieved.defaultParallelism shouldBe 4 // unchanged
    retrieved.threadPoolSize shouldBe 4 // unchanged
  }

  it should "maintain thread-safe configuration access" in {
    val conf1 = SparkletConf().copy(maxTaskRetries = 10)
    val conf2 = SparkletConf().copy(maxTaskRetries = 20)

    // Test thread safety by setting different configurations
    SparkletConf.set(conf1)
    SparkletConf.get.maxTaskRetries shouldBe 10

    SparkletConf.set(conf2)
    SparkletConf.get.maxTaskRetries shouldBe 20
  }

  it should "validate configuration constraints" in {
    val defaultConf = SparkletConf()

    // Ensure timeout is reasonable for production
    defaultConf.taskTimeoutMs should be > 0L
    defaultConf.taskTimeoutMs should be <= 3600000L // 1 hour max

    // Ensure retry configuration is reasonable
    defaultConf.maxTaskRetries should be >= 0
    defaultConf.maxTaskRetries should be <= 10 // reasonable upper bound

    defaultConf.baseRetryDelayMs should be > 0L
    defaultConf.maxRetryDelayMs should be >= defaultConf.baseRetryDelayMs

    // Ensure speculative execution threshold is reasonable
    defaultConf.speculativeExecutionThreshold should be >= 1.0
  }
}
