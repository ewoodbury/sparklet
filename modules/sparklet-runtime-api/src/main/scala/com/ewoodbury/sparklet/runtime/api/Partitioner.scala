package com.ewoodbury.sparklet.runtime.api

/**
  * Strategy interface for mapping keys to partition indices.
  */
trait Partitioner:
  /**
    * Returns a partition index in the range [0, numPartitions).
    */
  def partition(key: Any, numPartitions: Int): Int


