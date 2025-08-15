package com.ewoodbury.sparklet.runtime.local

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.ewoodbury.sparklet.core.BroadcastId
import com.ewoodbury.sparklet.runtime.api.BroadcastService

@SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
final class LocalBroadcastService extends BroadcastService:
  // Thread-safe storage for broadcast variables
  private val storage = new ConcurrentHashMap[BroadcastId, Seq[_]]()
  private val nextBroadcastId = new AtomicInteger(0)

  def broadcast[T](data: Seq[T]): BroadcastId =
    val id = BroadcastId(nextBroadcastId.getAndIncrement())
    storage.put(id, data)
    id

  def getBroadcast[T](id: BroadcastId): Seq[T] =
    Option(storage.get(id)) match
      case Some(data) => data.asInstanceOf[Seq[T]]
      case None => throw new IllegalArgumentException(s"Broadcast ID ${id.toInt} not found")

  def clear(): Unit = storage.clear()
