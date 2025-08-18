package com.ewoodbury.sparklet.api

import com.ewoodbury.sparklet.core.{ExecutionService, Partition, Plan}

/**
 * A lazy, immutable representation of a "distributed" collection. Operations build up a Plan,
 * execution is deferred until an action is called.
 *
 * This is the primary user-facing API for Sparklet, providing familiar transformation and action
 * operations for distributed data processing.
 *
 * @param plan
 *   The logical plan defining how to compute this collection.
 * @tparam A
 *   The type of elements in the collection.
 */
final case class DistCollection[A](plan: Plan[A]):

  // Initialize execution service on first use
  DistCollection.ensureExecutionServiceInitialized()

  // --- Helper to view the plan ---
  override def toString: String = s"DistCollection(plan = $plan)"

  // --- Basic Transformations ---

  /**
   * Transformation: Applies a mapping function lazily. Returns a new DistCollection representing
   * the result of the map. Does not trigger computation.
   */
  def map[B](f: A => B): DistCollection[B] =
    DistCollection(Plan.MapOp(this.plan, f))

  /**
   * Transformation: Applies a filter predicate lazily. Returns a new DistCollection representing
   * the filtered result. Does not trigger computation.
   */
  def filter(p: A => Boolean): DistCollection[A] =
    DistCollection(Plan.FilterOp(this.plan, p))

  /**
   * Transformation: Applies a flatMap function lazily. Returns a new DistCollection representing
   * the result of the flatMap. Does not trigger computation.
   */
  def flatMap[B](f: A => IterableOnce[B]): DistCollection[B] =
    DistCollection(Plan.FlatMapOp(this.plan, f))

  /**
   * Transformation: Applies a function to each partition as an iterator-in/iterator-out
   * transformation. Returns a new DistCollection representing the result. Does not trigger
   * computation.
   */
  def mapPartitions[B](f: Iterator[A] => Iterator[B]): DistCollection[B] =
    DistCollection(Plan.MapPartitionsOp(this.plan, f))

  /**
   * Transformation: Returns a new DistCollection with distinct elements. Does not trigger
   * computation.
   */
  def distinct(): DistCollection[A] =
    DistCollection(Plan.DistinctOp(this.plan))

  /**
   * Transformation: Returns a new DistCollection with the elements of two collections. Does not
   * trigger computation.
   */
  def union(other: DistCollection[A]): DistCollection[A] =
    DistCollection(Plan.UnionOp(this.plan, other.plan))

  /**
   * Transformation: Rebalances data across exactly `numPartitions` partitions (shuffle-based).
   * Returns a new DistCollection. Does not trigger computation.
   */
  def repartition(numPartitions: Int): DistCollection[A] =
    DistCollection(Plan.RepartitionOp(this.plan, numPartitions))

  /**
   * Transformation: Coalesces data into `numPartitions` partitions. Currently implemented via a
   * shuffle for simplicity. Returns a new DistCollection. Does not trigger computation.
   */
  def coalesce(numPartitions: Int): DistCollection[A] =
    DistCollection(Plan.CoalesceOp(this.plan, numPartitions))

  /**
   * Transformation: Explicitly partitions a key-value dataset by key using the runtime's
   * configured partitioner into `numPartitions` partitions.
   */
  def partitionBy[K, V](numPartitions: Int)(using ev: A =:= (K, V)): DistCollection[(K, V)] =
    DistCollection(Plan.PartitionByOp(this.plan.asInstanceOf[Plan[(K, V)]], numPartitions))

  // --- Key-Value Transformations ---

  /**
   * Transformation: Extracts the keys from the elements in the collection. Returns a new
   * DistCollection representing the keys. Does not trigger computation.
   */
  def keys[K, V](using ev: A =:= (K, V)): DistCollection[K] =
    DistCollection(Plan.KeysOp(this.plan.asInstanceOf[Plan[(K, V)]]))

  /**
   * Transformation: Extracts the values from the elements in the collection. Returns a new
   * DistCollection representing the values. Does not trigger computation.
   */
  def values[K, V](using ev: A =:= (K, V)): DistCollection[V] =
    DistCollection(Plan.ValuesOp(this.plan.asInstanceOf[Plan[(K, V)]]))

  /**
   * Transformation: Applies a function to the values of the elements in the collection. Returns a
   * new DistCollection representing the result of the map. Does not trigger computation.
   */
  def mapValues[K, V, B](f: V => B)(using ev: A =:= (K, V)): DistCollection[(K, B)] =
    DistCollection(Plan.MapValuesOp(this.plan.asInstanceOf[Plan[(K, V)]], f))

  /**
   * Transformation: Filters the elements of the collection by the keys. Returns a new
   * DistCollection representing the filtered result. Does not trigger computation.
   */
  def filterKeys[K, V](p: K => Boolean)(using ev: A =:= (K, V)): DistCollection[(K, V)] =
    DistCollection(Plan.FilterKeysOp(this.plan.asInstanceOf[Plan[(K, V)]], p))

  /**
   * Transformation: Filters the elements of the collection by the values. Returns a new
   * DistCollection representing the filtered result. Does not trigger computation.
   */
  def filterValues[K, V](p: V => Boolean)(using ev: A =:= (K, V)): DistCollection[(K, V)] =
    DistCollection(Plan.FilterValuesOp(this.plan.asInstanceOf[Plan[(K, V)]], p))

  /**
   * Transformation: Applies a function to the values of the elements in the collection. Returns a
   * new DistCollection representing the result of the flatMap. Does not trigger computation.
   */
  def flatMapValues[K, V, B](f: V => IterableOnce[B])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, B)] =
    DistCollection(Plan.FlatMapValuesOp(this.plan.asInstanceOf[Plan[(K, V)]], f))

  // --- Wide Transformations (require shuffles) ---

  /**
   * Transformation: Groups values by key, requiring a shuffle operation. Returns a new
   * DistCollection representing the grouped data. Does not trigger computation.
   */
  def groupByKey[K, V](using ev: A =:= (K, V)): DistCollection[(K, Iterable[V])] =
    DistCollection(Plan.GroupByKeyOp(this.plan.asInstanceOf[Plan[(K, V)]]))

  /**
   * Transformation: Reduces values by key using the provided function, requiring a shuffle
   * operation. Returns a new DistCollection representing the reduced data. Does not trigger
   * computation.
   */
  def reduceByKey[K, V](op: (V, V) => V)(using ev: A =:= (K, V)): DistCollection[(K, V)] =
    DistCollection(Plan.ReduceByKeyOp(this.plan.asInstanceOf[Plan[(K, V)]], op))

  /**
   * Transformation: Sorts the collection by the specified key function, requiring a shuffle
   * operation. Returns a new DistCollection representing the sorted data. Does not trigger
   * computation.
   */
  def sortBy[B](keyFunc: A => B)(using ordering: Ordering[B]): DistCollection[A] =
    DistCollection(Plan.SortByOp(this.plan, keyFunc, ordering))

  /**
   * Transformation: Joins this collection with another collection by key, requiring a shuffle
   * operation. Returns a new DistCollection representing the joined data. Does not trigger
   * computation.
   */
  def join[K, V, W](other: DistCollection[(K, W)])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, (V, W))] =
    DistCollection(Plan.JoinOp(this.plan.asInstanceOf[Plan[(K, V)]], other.plan, None))

  /**
   * Transformation: Joins this collection with another collection by key using shuffle-hash join.
   * Returns a new DistCollection representing the joined data. Does not trigger computation.
   */
  def shuffleHashJoin[K, V, W](other: DistCollection[(K, W)])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, (V, W))] =
    DistCollection(
      Plan.JoinOp(
        this.plan.asInstanceOf[Plan[(K, V)]],
        other.plan,
        Some(Plan.JoinStrategy.ShuffleHash),
      ),
    )

  /**
   * Transformation: Joins this collection with another collection by key using sort-merge join.
   * Returns a new DistCollection representing the joined data. Does not trigger computation.
   */
  def sortMergeJoin[K, V, W](other: DistCollection[(K, W)])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, (V, W))] =
    DistCollection(
      Plan.JoinOp(
        this.plan.asInstanceOf[Plan[(K, V)]],
        other.plan,
        Some(Plan.JoinStrategy.SortMerge),
      ),
    )

  /**
   * Transformation: Joins this collection with another collection by key using broadcast-hash
   * join. Returns a new DistCollection representing the joined data. Does not trigger computation.
   */
  def broadcastJoin[K, V, W](other: DistCollection[(K, W)])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, (V, W))] =
    DistCollection(
      Plan.JoinOp(
        this.plan.asInstanceOf[Plan[(K, V)]],
        other.plan,
        Some(Plan.JoinStrategy.Broadcast),
      ),
    )

  /**
   * Transformation: Co-groups this collection with another collection by key, requiring a shuffle
   * operation. Returns a new DistCollection representing the co-grouped data. Does not trigger
   * computation.
   */
  def cogroup[K, V, W](other: DistCollection[(K, W)])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, (Iterable[V], Iterable[W]))] =
    DistCollection(Plan.CoGroupOp(this.plan.asInstanceOf[Plan[(K, V)]], other.plan))

  // --- Actions ---

  /**
   * Action: Executes the plan and returns the results as a single local Iterable. This triggers
   * the computation using the registered ExecutionService.
   */
  def collect(): Iterable[A] =
    ExecutionService.get.execute(this.plan)

  /**
   * Action: Executes the plan and returns the count of elements. This triggers computation.
   */
  def count(): Long =
    ExecutionService.get.count(this.plan)

  /**
   * Action: Executes the plan and returns the first n elements. This triggers computation.
   */
  def take(n: Int): List[A] =
    ExecutionService.get.take(this.plan, n).toList

  /**
   * Action: Executes the plan and returns the first element. This triggers computation.
   */
  def first(): A =
    take(1).headOption.getOrElse(throw new NoSuchElementException("Collection is empty"))

  /**
   * Action: Executes the plan and reduces all elements using the given function. This triggers
   * computation.
   */
  def reduce(op: (A, A) => A): A =
    collect().reduceOption(op).getOrElse(throw new NoSuchElementException("Collection is empty"))

  /**
   * Action: Executes the plan and folds all elements using the given initial value and function.
   * This triggers computation.
   */
  def fold(initial: A)(op: (A, A) => A): A = collect().fold(initial)(op)

  /**
   * Action: Executes the plan and aggregates the elements using the given functions. This triggers
   * computation.
   */
  def aggregate[B](zero: B)(seqOp: (B, A) => B, combOp: (B, B) => B): B =
    collect().foldLeft(zero)(seqOp)

  /**
   * Action: Executes the plan and applies the given function to each element. This triggers
   * computation.
   */
  def foreach(f: A => Unit): Unit = collect().foreach(f)

  // --- Legacy Actions (these will be removed once DAG scheduler is implemented) ---
  // These actions still collect all data to the driver first. This is correct for
  // a local simulation but is not a distributed implementation.

  /**
   * Legacy action: Reduces by key by collecting all data to driver. Will be replaced by proper
   * shuffle-based implementation in DAG scheduler.
   */
  def reduceByKeyAction[K, V](op: (V, V) => V)(using ev: A =:= (K, V)): Map[K, V] =
    val collected = this.asInstanceOf[DistCollection[(K, V)]].collect()
    collected
      .groupBy(_._1)
      .map { case (k, pairs) =>
        (
          k,
          pairs
            .map(_._2)
            .reduceOption(op)
            .getOrElse(throw new NoSuchElementException(s"No values found for key $k")),
        )
      }

  /**
   * Legacy action: Groups by key by collecting all data to driver. Will be replaced by proper
   * shuffle-based implementation in DAG scheduler.
   */
  def groupByKeyAction[K, V](using ev: A =:= (K, V)): Map[K, Iterable[V]] =
    val collected = this.asInstanceOf[DistCollection[(K, V)]].collect()
    collected
      .groupBy(_._1)
      .map { case (k, pairs) => (k, pairs.map(_._2)) }

end DistCollection

// Companion object for creating a DistCollection from a source
object DistCollection:
  // Lazy initialization to ensure execution service is available
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var initialized = false

  private def ensureExecutionServiceInitialized(): Unit = {
    if (!initialized) {
      synchronized {
        if (!initialized) {
          // Try to initialize the execution module if available on classpath
          try {
            val clazz = Class.forName("com.ewoodbury.sparklet.execution.ExecutionModuleInit")
            val method = clazz.getMethod("initialize")
            method.invoke(null)
          } catch {
            case _: ClassNotFoundException =>
              // Execution module not on classpath - user must provide their own ExecutionService
              ()
            case _: Exception =>
              // Other initialization errors - log but continue
              ()
          }
          initialized = true
        }
      }
    }
  }

  /**
   * Creates a DistCollection from an existing Iterable data source, splitting it into a specified
   * number of partitions.
   *
   * @param data
   *   The source data.
   * @param numPartitions
   *   The desired number of partitions.
   * @return
   *   A new DistCollection.
   */
  def apply[A](data: Iterable[A], numPartitions: Int): DistCollection[A] =
    require(numPartitions > 0, "Number of partitions must be positive.")

    // Split the source data into groups that will become our partitions
    val groupedData = data.toSeq.grouped(math.ceil(data.size.toDouble / numPartitions).toInt)

    // Create the actual Partition objects
    val partitions = groupedData.map(chunk => Partition(chunk)).toSeq

    // Create the DistCollection, starting its logical plan with a Source node
    DistCollection(Plan.Source(partitions))
