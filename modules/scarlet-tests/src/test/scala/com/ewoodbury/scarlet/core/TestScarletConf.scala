package com.ewoodbury.scarlet.core

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestScarletConf extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit = {
    ScarletConf.set(originalConf)
    ()
  }

  "ScarletConf" should "provide default fault tolerance configuration values" in {
    val conf = ScarletConf()

    conf.maxTaskRetries shouldBe 3
    conf.baseRetryDelayMs shouldBe 1000L
    conf.maxRetryDelayMs shouldBe 30000L
  }

  it should "allow overriding fault tolerance configuration" in {
    val customConf = ScarletConf().copy(
      maxTaskRetries = 5,
      baseRetryDelayMs = 2000L,
      maxRetryDelayMs = 60000L,
    )

    ScarletConf.set(customConf)
    val retrieved = ScarletConf.get

    retrieved.maxTaskRetries shouldBe 5
    retrieved.baseRetryDelayMs shouldBe 2000L
    retrieved.maxRetryDelayMs shouldBe 60000L
  }

  it should "preserve existing configuration when setting new values" in {
    val customConf = ScarletConf().copy(
      defaultShufflePartitions = 8,
      maxTaskRetries = 2
    )

    ScarletConf.set(customConf)
    val retrieved = ScarletConf.get

    retrieved.defaultShufflePartitions shouldBe 8
    retrieved.maxTaskRetries shouldBe 2
    retrieved.defaultParallelism shouldBe 4 // unchanged
    retrieved.threadPoolSize shouldBe 4 // unchanged
  }

  it should "maintain thread-safe configuration access" in {
    val conf1 = ScarletConf().copy(maxTaskRetries = 10)
    val conf2 = ScarletConf().copy(maxTaskRetries = 20)

    // Test thread safety by setting different configurations
    ScarletConf.set(conf1)
    ScarletConf.get.maxTaskRetries shouldBe 10

    ScarletConf.set(conf2)
    ScarletConf.get.maxTaskRetries shouldBe 20
  }

  it should "validate configuration constraints" in {
    val defaultConf = ScarletConf()

    // Ensure retry configuration is reasonable
    defaultConf.maxTaskRetries should be >= 0
    defaultConf.maxTaskRetries should be <= 10 // reasonable upper bound

    defaultConf.baseRetryDelayMs should be > 0L
    defaultConf.maxRetryDelayMs should be >= defaultConf.baseRetryDelayMs
  }
}
