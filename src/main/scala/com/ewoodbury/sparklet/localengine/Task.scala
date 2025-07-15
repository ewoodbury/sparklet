package com.ewoodbury.sparklet.localengine

/**
 * A Task represents a unit of computation that can be run on an executor.
 * It operates on a single input partition to produce a single output partition.
 */
sealed trait Task[A, B]:
  def partition: Partition[A]
  def run(): Partition[B] // The execution logic

object Task:
  /** A task that applies a map function to a partition. */
  case class MapTask[A, B](
    partition: Partition[A], 
    f: A => B
  ) extends Task[A, B]:
    override def run(): Partition[B] = {
      println(s"[Thread: ${Thread.currentThread().getName}] Running MapTask on partition...")
      Partition(partition.data.map(f))
    }

  /** A task that applies a filter function to a partition. */
  case class FilterTask[A](
    partition: Partition[A], 
    p: A => Boolean
  ) extends Task[A, A]:
    override def run(): Partition[A] = {
      println(s"[Thread: ${Thread.currentThread().getName}] Running FilterTask on partition...")
      Partition(partition.data.filter(p))
    }
  
  /** A task that applies a flatMap function to a partition. */
  case class FlatMapTask[A, B](
    partition: Partition[A], 
    f: A => IterableOnce[B]
  ) extends Task[A, B]:
    override def run(): Partition[B] = {
      println(s"[Thread: ${Thread.currentThread().getName}] Running FlatMapTask on partition...")
      Partition(partition.data.flatMap(f))
    }

end Task