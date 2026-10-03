package com.ewoodbury.scarlet.columnar

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec

/**
 * Runs one worker per morsel. Each thread takes indexes from its own deque and steals from the
 * others, so one long morsel does not keep the rest of the queue behind it.
 *
 * Results line up with `morsels`. A worker failure, including an `Error`, fails the call with that
 * throwable. `parallelism` of `1` runs on the calling thread.
 */
object MorselScheduler:

  def map[A](morsels: Vector[ColumnBatch], parallelism: Int)(
      worker: (Int, ColumnBatch) => A,
  ): Vector[A] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (morsels.isEmpty) then Vector.empty
    else if (parallelism == 1 || morsels.length == 1) then
      morsels.iterator.zipWithIndex.map { (batch, index) => worker(index, batch) }.toVector
    else
      val workers = math.min(parallelism, morsels.length)
      val deques = Array.fill(workers)(new ConcurrentLinkedDeque[Int])
      morsels.indices.foreach(index => deques(index % workers).addLast(index))
      val slots = Array.fill(morsels.length)(new AtomicReference[Option[A]](None))
      val failure = new AtomicReference[Option[Throwable]](None)
      val threads = Array.tabulate(workers) { id =>
        val thread = new Thread(
          () => drain(id, deques, slots, failure, morsels, worker),
          s"scarlet-morsel-$id",
        )
        thread.setDaemon(true)
        thread
      }
      threads.foreach(_.start())
      threads.foreach(_.join())
      failure.get() match
        case Some(thrown) => throw thrown
        case None => ()
      slots.iterator.map { slot =>
        slot.get() match
          case Some(value) => value
          case None => throw new IllegalStateException("morsel finished without a result")
      }.toVector

  @tailrec
  private def drain[A](
      id: Int,
      deques: Array[ConcurrentLinkedDeque[Int]],
      slots: Array[AtomicReference[Option[A]]],
      failure: AtomicReference[Option[Throwable]],
      morsels: Vector[ColumnBatch],
      worker: (Int, ColumnBatch) => A,
  ): Unit =
    if (failure.get().isEmpty) then
      take(id, deques, failure) match
        case None => ()
        case Some(index) =>
          val batch = morsels.lift(index).getOrElse {
            throw new IllegalArgumentException(s"missing morsel $index")
          }
          try slots(index).set(Some(worker(index, batch)))
          catch case thrown: Throwable => failure.compareAndSet(None, Some(thrown))
          drain(id, deques, slots, failure, morsels, worker)

  private def take(
      id: Int,
      deques: Array[ConcurrentLinkedDeque[Int]],
      failure: AtomicReference[Option[Throwable]],
  ): Option[Int] =
    if (failure.get().nonEmpty) then None
    else Option(deques(id).pollFirst()).orElse(steal(id, deques))

  private def steal(id: Int, deques: Array[ConcurrentLinkedDeque[Int]]): Option[Int] =
    deques.indices.iterator
      .filter(_ != id)
      .map(other => Option(deques(other).pollLast()))
      .collectFirst { case Some(index) => index }
