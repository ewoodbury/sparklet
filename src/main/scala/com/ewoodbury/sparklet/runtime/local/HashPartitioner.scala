package com.ewoodbury.sparklet.runtime.local

import com.ewoodbury.sparklet.runtime.api.Partitioner

final class HashPartitioner extends Partitioner:
  def partition(key: Any, numPartitions: Int): Int =
    if numPartitions <= 0 then 0 else math.abs(key.hashCode()) % numPartitions
