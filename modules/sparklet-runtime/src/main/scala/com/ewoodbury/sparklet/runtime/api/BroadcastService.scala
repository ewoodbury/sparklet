package com.ewoodbury.sparklet.runtime.api

import com.ewoodbury.sparklet.core.BroadcastId

/**
 * Service for broadcasting small datasets to all nodes in a cluster. For the local implementation,
 * this simply stores data in memory.
 */
trait BroadcastService:
  /**
   * Broadcast a small dataset and return a broadcast ID for later reference.
   */
  def broadcast[T](data: Seq[T]): BroadcastId

  /**
   * Retrieve broadcast data by ID.
   */
  def getBroadcast[T](id: BroadcastId): Seq[T]

  /**
   * Clean up broadcast data (primarily for testing).
   */
  def clear(): Unit
