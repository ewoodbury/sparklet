package com.ewoodbury.sparklet.core

import com.ewoodbury.sparklet.execution.{Executor, Task, TaskScheduler}

/**
 * A lazy, immutable representation of a "distributed" collection.
 * Operations build up a Plan, execution is deferred until an action is called.
 *
 * @param plan The logical plan defining how to compute this collection.
 * @tparam A The type of elements in the collection.
 */
final case class DistCollection[A](plan: Plan[A]):

  // --- Helper to view the plan ---
  override def toString: String = s"DistCollection(plan = $plan)"

  // --- Basic Transformations ---

  /**
   * Transformation: Applies a mapping function lazily.
   * Returns a new DistCollection representing the result of the map.
   * Does not trigger computation.
   */
  def map[B](f: A => B): DistCollection[B] =
    DistCollection(Plan.MapOp(this.plan, f))

  /**
   * Transformation: Applies a filter predicate lazily.
   * Returns a new DistCollection representing the filtered result.
   * Does not trigger computation.
   */
  def filter(p: A => Boolean): DistCollection[A] =
    DistCollection(Plan.FilterOp(this.plan, p))

  /**
   * Transformation: Applies a flatMap function lazily.
   * Returns a new DistCollection representing the result of the flatMap.
   * Does not trigger computation.
   */
  def flatMap[B](f: A => IterableOnce[B]): DistCollection[B] =
    DistCollection(Plan.FlatMapOp(this.plan, f))

  /**
   * Transformation: Returns a new DistCollection with distinct elements.
   * Does not trigger computation.
   */
  def distinct(): DistCollection[A] =
    DistCollection(Plan.DistinctOp(this.plan))

  /**
   * Transformation: Returns a new DistCollection with the elements of two collections.
   * Does not trigger computation.
   */
  def union(other: DistCollection[A]): DistCollection[A] =
    DistCollection(Plan.UnionOp(this.plan, other.plan))

  // --- Key-Value Transformations ---

  /**
    * Transformation: Extracts the keys from the elements in the collection.
    * Returns a new DistCollection representing the keys.
    * Does not trigger computation.
    */
  def keys[K, V](using ev: A =:= (K, V)): DistCollection[K] =
    DistCollection(Plan.KeysOp(this.plan.asInstanceOf[Plan[(K, V)]]))

  /**
    * Transformation: Extracts the values from the elements in the collection.
    * Returns a new DistCollection representing the values.
    * Does not trigger computation.
    */
  def values[K, V](using ev: A =:= (K, V)): DistCollection[V] =
    DistCollection(Plan.ValuesOp(this.plan.asInstanceOf[Plan[(K, V)]]))

  /**
   * Transformation: Applies a function to the values of the elements in the collection.
   * Returns a new DistCollection representing the result of the map.
   * Does not trigger computation.
   */
  def mapValues[K, V, B](f: V => B)(using ev: A =:= (K, V)): DistCollection[(K, B)] =
    DistCollection(Plan.MapValuesOp(this.plan.asInstanceOf[Plan[(K, V)]], f))

  /**
   * Transformation: Filters the elements of the collection by the keys.
   * Returns a new DistCollection representing the filtered result.
   * Does not trigger computation.
   */
  def filterKeys[K, V](p: K => Boolean)(using ev: A =:= (K, V)): DistCollection[(K, V)] =
    DistCollection(Plan.FilterKeysOp(this.plan.asInstanceOf[Plan[(K, V)]], p))

  /**
   * Transformation: Filters the elements of the collection by the values.
   * Returns a new DistCollection representing the filtered result.
   * Does not trigger computation.
   */
  def filterValues[K, V](p: V => Boolean)(using ev: A =:= (K, V)): DistCollection[(K, V)] =
    DistCollection(Plan.FilterValuesOp(this.plan.asInstanceOf[Plan[(K, V)]], p))

  /**
   * Transformation: Applies a function to the values of the elements in the collection.
   * Returns a new DistCollection representing the result of the flatMap.
   * Does not trigger computation.
   */
  def flatMapValues[K, V, B](f: V => IterableOnce[B])(using ev: A =:= (K, V)): DistCollection[(K, B)] =
    DistCollection(Plan.FlatMapValuesOp(this.plan.asInstanceOf[Plan[(K, V)]], f))


  // --- Actions ---

  /**
   * Action: Executes the plan and returns the results as a single local Iterable.
   * This triggers the computation using the concurrent TaskScheduler.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def collect(): Iterable[A] = {
    println("--- Collect Action Triggered ---")

    // Use the task-based execution path for all operations
    this.plan match {
      case s: Plan.Source[A] =>
        // Sources don't need tasks, just return the data directly
        s.partitions.flatMap(_.data)
        
      case _ =>
        val tasks = Executor.createTasks(this.plan)
        // Cast to the expected type for TaskScheduler - this is safe because createTasks
        // returns tasks that produce the correct output type A
        val typedTasks = tasks.asInstanceOf[Seq[Task[Any, A]]]

        val resultPartitions = TaskScheduler.submit(typedTasks)
        resultPartitions.flatMap(_.data)
    }
  }
  
  // All other actions (count, take, reduce, etc.) can now be defined
  // in terms of collect() for simplicity.
  
  def count(): Long =
    println("--- Count Action Triggered ---")
    collect().size.toLong

  def take(n: Int): List[A] =
    println("--- Take Action Triggered ---")
    // This is inefficient as it collects everything first. A real implementation
    // would have a more optimized executor for `take`.
    collect().take(n).toList
    
  def first(): A = take(1).headOption.getOrElse(throw new NoSuchElementException("Collection is empty"))
  
  def reduce(op: (A, A) => A): A = collect().reduceOption(op).getOrElse(throw new NoSuchElementException("Collection is empty"))
  
  def fold(initial: A)(op: (A, A) => A): A = collect().fold(initial)(op)

  def aggregate[B](zero: B)(seqOp: (B, A) => B, combOp: (B, B) => B): B =
    collect().aggregate(zero)(seqOp, combOp)
  
  def foreach(f: A => Unit): Unit = collect().foreach(f)

  // These actions still collect all data to the driver first. This is correct for
  // a local simulation but is not a distributed implementation.
  def reduceByKey[K, V](op: (V, V) => V)(using ev: A =:= (K, V)): Map[K, V] =
    val collected = this.asInstanceOf[DistCollection[(K, V)]].collect()
    collected
      .groupBy(_._1)
      .map { case (k, pairs) => (k, pairs.map(_._2).reduceOption(op).getOrElse(throw new NoSuchElementException("Collection is empty"))) }

  def groupByKey[K, V](using ev: A =:= (K, V)): Map[K, Iterable[V]] =
    val collected = this.asInstanceOf[DistCollection[(K, V)]].collect()
    collected
      .groupBy(_._1)
      .map { case (k, pairs) => (k, pairs.map(_._2)) }

end DistCollection

// Companion object for creating a DistCollection from a source
object DistCollection:
  /**
   * Creates a DistCollection from an existing Iterable data source, splitting it
   * into a specified number of partitions.
   *
   * @param data The source data.
   * @param numPartitions The desired number of partitions.
   * @return A new DistCollection.
   */
  def apply[A](data: Iterable[A], numPartitions: Int): DistCollection[A] =
    require(numPartitions > 0, "Number of partitions must be positive.")
    
    // Split the source data into groups that will become our partitions
    val groupedData = data.toSeq.grouped(math.ceil(data.size.toDouble / numPartitions).toInt)
    
    // Create the actual Partition objects
    val partitions = groupedData.map(chunk => Partition(chunk)).toSeq
    
    // Create the DistCollection, starting its logical plan with a Source node
    DistCollection(Plan.Source(partitions))