package com.ewoodbury.sparklet.localengine

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
   * Applies a mapping function lazily.
   * Returns a new DistCollection representing the result of the map.
   * Does not trigger computation.
   */
  def map[B](f: A => B): DistCollection[B] =
    DistCollection(Plan.MapOp(this.plan, f))

  /**
   * Applies a filter predicate lazily.
   * Returns a new DistCollection representing the filtered result.
   * Does not trigger computation.
   */
  def filter(p: A => Boolean): DistCollection[A] =
    DistCollection(Plan.FilterOp(this.plan, p))

  /**
   * Applies a flatMap function lazily.
   * Returns a new DistCollection representing the result of the flatMap.
   * Does not trigger computation.
   */
  def flatMap[B](f: A => IterableOnce[B]): DistCollection[B] =
    DistCollection(Plan.FlatMapOp(this.plan, f))

  def distinct(): DistCollection[A] =
    DistCollection(Plan.DistinctOp(this.plan))

  def union(other: DistCollection[A]): DistCollection[A] =
    DistCollection(Plan.UnionOp(this.plan, other.plan))

  // --- Key-Value Transformations ---

  def mapValues[K, V, B](f: V => B)(using ev: A =:= (K, V)): DistCollection[(K, B)] =
    DistCollection(Plan.MapValuesOp(this.plan.asInstanceOf[Plan[(K, V)]], f))

  def filterKeys[K, V](p: K => Boolean)(using ev: A =:= (K, V)): DistCollection[(K, V)] =
    DistCollection(Plan.FilterKeysOp(this.plan.asInstanceOf[Plan[(K, V)]], p))

  def flatMapValues[K, V, B](f: V => IterableOnce[B])(using ev: A =:= (K, V)): DistCollection[(K, B)] =
    DistCollection(Plan.FlatMapValuesOp(this.plan.asInstanceOf[Plan[(K, V)]], f))
  
  
  // --- TODO: Add more transformations here ---

  // --- Actions ---

  /**
   * Action: Executes the plan using the LocalExecutor and returns the results.
   * This triggers the computation defined by the plan.
   */
  def collect(): Iterable[A] =
    println("--- Collect Action Triggered ---")
    LocalExecutor.execute(this.plan)

  /**
   * Action: Executes the plan to count the elements.
   */
  def count(): Long =
    println("--- Count Action Triggered ---")
    LocalExecutor.execute(this.plan).size.toLong // Naive implementation for local

  /**
   * Action: Executes the plan to take the first n elements.
   */
  def take(n: Int): DistCollection[A] =
    println("--- Take Action Triggered ---")
    DistCollection(Plan.Source(() => LocalExecutor.execute(this.plan).take(n)))

  def first(): A =
    println("--- First Action Triggered ---")
    LocalExecutor.execute(this.plan).headOption.get

  /**
   * Action: Executes the plan to reduce the elements.
   */
  def reduce(op: (A, A) => A): A =
    println("--- Reduce Action Triggered ---")
    LocalExecutor.execute(this.plan).reduceOption(op).get

  /**
   * Action: Executes the plan to fold the elements.
   */
  def fold(initial: A)(op: (A, A) => A): A =
    println("--- Fold Action Triggered ---")
    LocalExecutor.execute(this.plan).fold(initial)(op)

  def aggregate[B](zero: B)(seqOp: (B, A) => B, combOp: (B, B) => B): B =
    println("--- Aggregate Action Triggered ---")
    LocalExecutor.execute(this.plan).aggregate(zero)(seqOp, combOp)

  def foreach(f: A => Unit): Unit =
    println("--- ForEach Action Triggered ---")
    LocalExecutor.execute(this.plan).foreach(f)

  /**
   * Action: Executes the plan to reduce the elements by key.
   */
  def reduceByKey[K, V](op: (V, V) => V)(using ev: A =:= (K, V)): Map[K, V] =
    println("--- ReduceByKey Action Triggered ---")
    LocalExecutor.execute(this.plan.asInstanceOf[Plan[(K, V)]])
    .groupBy(_._1)
    .map { case (k, pairs) => 
      val values = pairs.map(_._2)
      (k, values.reduceOption(op).get)
    }.toMap

  /**
    * Action: Groups the elements by key.
    */
  def groupByKey[K, V](using ev: A =:= (K, V)): Map[K, Iterable[V]] =
    println("--- GroupByKey Action Triggered ---")
    LocalExecutor.execute(this.plan.asInstanceOf[Plan[(K, V)]])
    .groupBy(_._1)
    .map { case (k, pairs) => (k, pairs.map(_._2)) }

end DistCollection

// Companion object for easy creation from source data
object DistCollection:
  /**
   * Creates a DistCollection from an existing Iterable data source.
   */
  def apply[A](data: Iterable[A]): DistCollection[A] =
    // Store it as a function () => data for lazy access by the executor
    DistCollection(Plan.Source(() => data))