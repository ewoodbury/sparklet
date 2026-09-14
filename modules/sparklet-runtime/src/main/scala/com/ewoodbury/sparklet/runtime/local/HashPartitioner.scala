package com.ewoodbury.sparklet.runtime.local

import com.ewoodbury.sparklet.runtime.api.Partitioner

final class HashPartitioner extends Partitioner:
  /**
   * Assigns a key to a partition using a non-negative modulus, so negative hash codes (including
   * `Int.MinValue`) map to valid partition indexes.
   */
  def partition(key: Any, numPartitions: Int): Int =
    if numPartitions <= 0 then 0 else Math.floorMod(key.hashCode(), numPartitions)
