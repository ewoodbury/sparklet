package com.ewoodbury.sparklet.execution

/**
 * Physical description of a stage output: how rows are distributed, whether they are ordered, and
 * whether they are boxed rows or column batches.
 *
 * The row path always uses [[Layout.BoxedRows]]. `None` on a stage means the distribution was
 * dropped (union). A present value always names a width.
 *
 * Hash and range on this path use [[PartitioningInfo.RowKeyOrdinals]] (`Seq(0)`): the single
 * logical key (the pair key, or the sort key). That is not a schema column. Columnar execution
 * will substitute real ordinals; shuffle bypass does not compare ordinals yet.
 */
final case class PartitioningInfo(
    distribution: Distribution,
    ordering: OrderingTag,
    layout: Layout,
):

  def numPartitions: Int = distribution match {
    case Distribution.Unknown(n) => n
    case Distribution.Singleton => 1
    case Distribution.Hash(n, _) => n
    case Distribution.Range(n, _) => n
    case Distribution.RoundRobin(n) => n
  }

  /** Hash-partitioned at exactly this width. Key ordinals are not compared. */
  def isHashPartitioned(width: Int): Boolean = distribution match {
    case Distribution.Hash(n, _) => n == width
    case _ => false
  }

  /**
   * Same width, and not hash-partitioned. This is the old `byKey = false` check: repartition may
   * skip a shuffle for unknown, range, round-robin, or singleton outputs.
   */
  def matchesNonHashWidth(width: Int): Boolean = distribution match {
    case Distribution.Hash(_, _) => false
    case _ => numPartitions == width
  }

  /**
   * Drop a hash guarantee when the output is no longer a key-value pair (`keys` / `values`). Other
   * distributions stay. The elements may still be co-located; bypass only trusts pair-key hash.
   */
  def withoutHashDistribution: PartitioningInfo = distribution match {
    case Distribution.Hash(n, _) => copy(distribution = Distribution.Unknown(n))
    case _ => this
  }

  /** `None` when this value is safe to put on a stage. */
  def invalidReason: Option[String] = {
    val distributionReason = distribution match {
      case Distribution.Unknown(n) if n < 0 =>
        Some(s"Unknown distribution has numPartitions=$n < 0")
      case Distribution.Hash(n, _) if n <= 0 =>
        Some(s"Hash distribution has numPartitions=$n <= 0")
      case Distribution.Hash(_, keys) if keys.isEmpty =>
        Some("Hash distribution has no key ordinals")
      case Distribution.Range(n, _) if n <= 0 =>
        Some(s"Range distribution has numPartitions=$n <= 0")
      case Distribution.Range(_, keys) if keys.isEmpty =>
        Some("Range distribution has no key ordinals")
      case Distribution.RoundRobin(n) if n <= 0 =>
        Some(s"RoundRobin distribution has numPartitions=$n <= 0")
      case _ => None
    }

    val widthReason =
      if (numPartitions > PartitioningInfo.MaxPartitions) {
        Some(
          s"numPartitions=$numPartitions exceeds ${PartitioningInfo.MaxPartitions}",
        )
      } else {
        None
      }

    val layoutReason = layout match {
      case Layout.Columnar(batchSize) if batchSize <= 0 =>
        Some(s"Columnar layout has batchSize=$batchSize <= 0")
      case _ => None
    }

    val orderingReason = ordering match {
      case OrderingTag.Sorted(keys, _) if keys.isEmpty =>
        Some("Sorted ordering has no key ordinals")
      case OrderingTag.Sorted(keys, ascending)
          if ascending.nonEmpty && ascending.length != keys.length =>
        Some("Sorted ordering ascending flags do not match key ordinals")
      case _ => None
    }

    distributionReason.orElse(widthReason).orElse(layoutReason).orElse(orderingReason)
  }

/**
 * How rows are spread across the output partitions.
 *
 * [[Distribution.Unknown]] carries a width. Sources and operations that drop a key partitioner
 * know how many partitions they have and nothing else. Missing metadata is `Option.None`, not
 * `Unknown`.
 */
enum Distribution:
  case Unknown(numPartitions: Int)
  case Singleton
  case Hash(numPartitions: Int, keyOrdinals: Seq[Int])
  case Range(numPartitions: Int, keyOrdinals: Seq[Int])
  case RoundRobin(numPartitions: Int)

/** Order of rows inside the output. Empty `ascending` means the direction is not known. */
enum OrderingTag:
  case None
  case Sorted(keyOrdinals: Seq[Int], ascending: Seq[Boolean])

/** Physical batching of the output. The row engine emits [[Layout.BoxedRows]] only. */
enum Layout:
  case BoxedRows
  case Columnar(batchSize: Int)

object PartitioningInfo:

  /** Upper bound enforced by stage-graph validation. */
  val MaxPartitions: Int = 1000000

  /**
   * Stand-in ordinal for the row path's single logical key. Not a column index.
   */
  val RowKeyOrdinals: Seq[Int] = Seq(0)

  def unknown(numPartitions: Int): PartitioningInfo =
    PartitioningInfo(
      distribution = Distribution.Unknown(numPartitions),
      ordering = OrderingTag.None,
      layout = Layout.BoxedRows,
    )

  def hash(numPartitions: Int): PartitioningInfo =
    PartitioningInfo(
      distribution = Distribution.Hash(numPartitions, RowKeyOrdinals),
      ordering = OrderingTag.None,
      layout = Layout.BoxedRows,
    )

  /**
   * Range-partitioned sort output. `ascending` is empty: a row-path `Ordering` does not expose
   * direction.
   */
  def rangeSorted(numPartitions: Int): PartitioningInfo =
    PartitioningInfo(
      distribution = Distribution.Range(numPartitions, RowKeyOrdinals),
      ordering = OrderingTag.Sorted(keyOrdinals = RowKeyOrdinals, ascending = Seq.empty[Boolean]),
      layout = Layout.BoxedRows,
    )

  def roundRobin(numPartitions: Int): PartitioningInfo =
    PartitioningInfo(
      distribution = Distribution.RoundRobin(numPartitions),
      ordering = OrderingTag.None,
      layout = Layout.BoxedRows,
    )
