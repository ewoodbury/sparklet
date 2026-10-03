package com.ewoodbury.sparklet.execution

import scala.annotation.tailrec

import com.ewoodbury.sparklet.columnar.*
import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf}

/**
 * Lowers `Int`, `(Int, Int)`, and `String` plans to [[PhysicalOp]] and runs that tree on the
 * columnar kernels.
 *
 * Named erasure boundary. `Plan` is invariant and its element type is erased, so a source is
 * classified by reading one value, and user functions are then cast to that operation. A function
 * that does not return the selected type throws `ClassCastException`. [[tryRun]] catches that and
 * returns `None`, and the row path runs the whole plan. Any unsupported node (union, flatMap,
 * distinct, sort, a map that is not fed by `Int` or `String`, and the rest) makes the whole plan
 * ineligible, so a supported prefix is not executed on its own.
 *
 * [[lower]] and interpretation walk the same logical plan. The physical node records the operator
 * and not the function, so each logical node is matched to the node [[lower]] built for it. A
 * mismatch is a planner bug: interpretation throws `IllegalStateException`, and [[tryRun]] does
 * not turn that into a row-path fallback. A `ClassCastException` from a function that is not the
 * selected type still is a fallback, and user side effects in that function may already have run.
 *
 * [[lower]] does not call user functions. Interpretation is one kernel per node. Narrow map and
 * filter keep input order and the input partition count. `String` selects filter and map only. A
 * filter over a join result is not columnar. `reduceByKey` applies the user's function and wraps
 * at `Int`. `join` hash-partitions both sides and probes one bucket at a time. Output width for
 * those two is `defaultShufflePartitions`.
 *
 * A null string is a value. Classification skips a leading null and keeps looking for a `String`.
 * An all-null source stays on the row path, because there is no witness.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
  ),
)
object ColumnarPlanner:

  /** Layout the planner would use, without running the plan. `None` means the row path. */
  def partitioning[A](plan: Plan[A]): Option[PartitioningInfo] =
    accepted(plan).map(infoOf)

  /**
   * The physical tree for `plan`, without running it. `None` means the row path. An invalid layout
   * still lowers; [[partitioning]] and [[tryRun]] refuse it.
   */
  def lower(plan: Plan[_]): Option[PhysicalOp] =
    if (!SparkletConf.get.columnarExecution) then None
    else lowerPlan(plan)

  /**
   * Run `plan` on the columnar path. `None` means the caller should use the row path. The single
   * result cast is safe when selection succeeded: the decoded partitions have the plan's element
   * type.
   */
  def tryRun[A](plan: Plan[A]): Option[Seq[Partition[A]]] =
    accepted(plan) match
      case None => None
      case Some(op) =>
        try Some(decode(interpret(plan, op)).asInstanceOf[Seq[Partition[A]]])
        catch case _: ClassCastException => None

  private enum Columns:
    case Ints(parts: Vector[ColumnBatch])
    case Pairs(parts: Vector[ColumnBatch])
    case Triples(parts: Vector[ColumnBatch])
    case Strings(parts: Vector[ColumnBatch])

  private def accepted(plan: Plan[_]): Option[PhysicalOp] =
    lower(plan).filter(op => infoOf(op).invalidReason.isEmpty)

  private def lowerPlan(plan: Plan[_]): Option[PhysicalOp] = plan match
    case Plan.Source(partitions) =>
      classify(partitions).map(kind => PhysicalOp.Scan(kind, partitions))
    case Plan.FilterOp(source, _) =>
      lowerPlan(source).flatMap(child => filterOf(child))
    case Plan.MapOp(source, _) =>
      lowerPlan(source).flatMap { child =>
        child.kind match
          case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Strings =>
            Some(PhysicalOp.Project(child))
          case _ => None
      }
    case Plan.FilterKeysOp(source, _) =>
      columnFilter(source, PhysicalOp.Slot.Key)
    case Plan.FilterValuesOp(source, _) =>
      columnFilter(source, PhysicalOp.Slot.Value)
    case Plan.ReduceByKeyOp(source, _) =>
      lowerPlan(source).flatMap { child =>
        child.kind match
          case PhysicalOp.Kind.Pairs =>
            Some(PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)))
          case _ => None
      }
    case Plan.JoinOp(left, right, _) =>
      (lowerPlan(left), lowerPlan(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)))
            case _ => None
        case _ => None
    case _ => None

  private def filterOf(child: PhysicalOp): Option[PhysicalOp] =
    child.kind match
      case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Strings =>
        Some(PhysicalOp.Filter(child, PhysicalOp.Slot.Element))
      case PhysicalOp.Kind.Pairs =>
        Some(PhysicalOp.Filter(child, PhysicalOp.Slot.Row))
      case PhysicalOp.Kind.Triples => None

  private def columnFilter(source: Plan[_], slot: PhysicalOp.Slot): Option[PhysicalOp] =
    lowerPlan(source).flatMap { child =>
      child.kind match
        case PhysicalOp.Kind.Pairs => Some(PhysicalOp.Filter(child, slot))
        case _ => None
    }

  private def classify(partitions: Seq[Partition[_]]): Option[PhysicalOp.Kind] =
    if (partitions.isEmpty) then None
    else
      val values = partitions.iterator.flatMap(_.data.iterator)
      values.nextOption().flatMap(value => classifyValue(value, values))

  /** The first present value selects the kind. A leading empty cell can only witness a string. */
  private def classifyValue(value: Any, rest: Iterator[Any]): Option[PhysicalOp.Kind] =
    Option(value) match
      case Some(_: Int) => Some(PhysicalOp.Kind.Ints)
      case Some((_: Int, _: Int)) => Some(PhysicalOp.Kind.Pairs)
      case Some(_: String) => Some(PhysicalOp.Kind.Strings)
      case Some(_) => None
      case None => stringWitness(rest)

  @tailrec
  private def stringWitness(values: Iterator[Any]): Option[PhysicalOp.Kind] =
    values.nextOption() match
      case None => None
      case Some(value) =>
        Option(value) match
          case None => stringWitness(values)
          case Some(_: String) => Some(PhysicalOp.Kind.Strings)
          case Some(_) => None

  private def infoOf(op: PhysicalOp): PartitioningInfo = op match
    case PhysicalOp.Filter(child, _) => infoOf(child)
    case PhysicalOp.Project(child) => infoOf(child)
    case _: PhysicalOp.HashAggregate | _: PhysicalOp.HashJoin => hashed
    case PhysicalOp.Exchange(_) =>
      throw new IllegalStateException("exchange is only a child of aggregate or join")
    case PhysicalOp.Scan(_, partitions) =>
      narrow(Distribution.Unknown(partitions.length))

  private def interpret(plan: Plan[_], op: PhysicalOp): Columns = (plan, op) match
    case (Plan.Source(partitions), PhysicalOp.Scan(PhysicalOp.Kind.Ints, _)) =>
      Columns.Ints(encodeInts(partitions))
    case (Plan.Source(partitions), PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) =>
      Columns.Pairs(encodePairs(partitions))
    case (Plan.Source(partitions), PhysicalOp.Scan(PhysicalOp.Kind.Strings, _)) =>
      Columns.Strings(encodeStrings(partitions))
    case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, slot)) =>
      applyFilter(interpret(source, child), predicate, slot)
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child)) =>
      project(interpret(source, child), mapper)
    case (Plan.FilterKeysOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Key)) =>
      applyFilter(interpret(source, child), predicate, PhysicalOp.Slot.Key)
    case (
          Plan.FilterValuesOp(source, predicate),
          PhysicalOp.Filter(child, PhysicalOp.Slot.Value),
        ) =>
      applyFilter(interpret(source, child), predicate, PhysicalOp.Slot.Value)
    case (
          Plan.ReduceByKeyOp(source, reduceFunc),
          PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)),
        ) =>
      val op = reduceFunc.asInstanceOf[(Int, Int) => Int]
      Columns.Pairs(
        HashReduce.byKey(
          pairsOf(interpret(source, child)),
          shuffleWidth,
          parallelism,
          batchSize,
          (left, right) => op(left, right),
        ),
      )
    case (
          Plan.JoinOp(left, right, _),
          PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
        ) =>
      Columns.Triples(
        ColumnPipeline.innerJoin(
          pairsOf(interpret(left, leftOp)),
          pairsOf(interpret(right, rightOp)),
          shuffleWidth,
          parallelism,
          batchSize,
        ),
      )
    case _ =>
      throw new IllegalStateException(
        s"physical tree does not match ${plan.getClass.getSimpleName}",
      )

  private def applyFilter(columns: Columns, predicate: Any, slot: PhysicalOp.Slot): Columns =
    slot match
      case PhysicalOp.Slot.Element =>
        columns match
          case Columns.Ints(parts) =>
            val keep = predicate.asInstanceOf[Int => Boolean]
            Columns.Ints(onEach(parts) { batch =>
              ColumnKernel.filterInt32(batch, 0, value => keep(value))
            })
          case Columns.Strings(parts) =>
            val keep = predicate.asInstanceOf[String => Boolean]
            Columns.Strings(onEach(parts) { batch =>
              ColumnKernel.filterUtf8(batch, 0, value => keep(value))
            })
          case _ =>
            throw new IllegalStateException("columnar element filter expects ints or strings")
      case PhysicalOp.Slot.Row =>
        val keep = predicate.asInstanceOf[((Int, Int)) => Boolean]
        // `keep` is `(Int, Int) => Boolean`, so the pair is boxed for the call.
        Columns.Pairs(onEach(pairsOf(columns)) { batch =>
          PairKernel.filter(batch, (key, value) => keep((key, value)))
        })
      case PhysicalOp.Slot.Key =>
        val keep = predicate.asInstanceOf[Int => Boolean]
        Columns.Pairs(onEach(pairsOf(columns)) { batch =>
          ColumnKernel.filterInt32(batch, 0, value => keep(value))
        })
      case PhysicalOp.Slot.Value =>
        val keep = predicate.asInstanceOf[Int => Boolean]
        Columns.Pairs(onEach(pairsOf(columns)) { batch =>
          ColumnKernel.filterInt32(batch, 1, value => keep(value))
        })

  private def project(columns: Columns, mapper: Any): Columns = columns match
    case Columns.Ints(parts) =>
      val fn = mapper.asInstanceOf[Int => Int]
      Columns.Ints(onEach(parts) { batch =>
        ColumnKernel.projectInt32(batch, 0, value => fn(value))
      })
    case Columns.Strings(parts) =>
      val fn = mapper.asInstanceOf[String => Any]
      Columns.Strings(onEach(parts) { batch =>
        ColumnKernel.projectUtf8(batch, 0, value => stringResult(fn(value)))
      })
    case _ =>
      throw new IllegalStateException("columnar map expects ints or strings")

  private def pairsOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Pairs(parts) if parts.nonEmpty => parts
    case Columns.Pairs(_) =>
      throw new IllegalStateException("columnar pairs have no partitions")
    case _ =>
      throw new IllegalStateException("columnar op expected int pairs")

  private def onEach(
      parts: Vector[ColumnBatch],
  )(fn: ColumnBatch => ColumnBatch): Vector[ColumnBatch] =
    if (parts.isEmpty) then parts
    else
      val sliced = parts.map(part => ColumnBatches.slice(part, batchSize))
      val flat = sliced.flatten
      val mapped = MorselScheduler.map(flat, parallelism)((_, morsel) => fn(morsel))
      regroup(sliced.map(_.length), mapped)

  private def regroup(counts: Vector[Int], flat: Vector[ColumnBatch]): Vector[ColumnBatch] =
    val seed: (Vector[ColumnBatch], Vector[ColumnBatch]) = (flat, Vector.empty[ColumnBatch])
    val (_, grouped) = counts.foldLeft(seed) { (state, count) =>
      val (rest, acc) = state
      val (group, tail) = rest.splitAt(count)
      (tail, acc :+ ColumnBatches.concat(group))
    }
    grouped

  private def encodeInts(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    partitions.map { partition =>
      BatchCodec.ints(partition.data.asInstanceOf[Iterable[Int]])
    }.toVector

  private def encodePairs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    partitions.map { partition =>
      BatchCodec.intPairs(partition.data.asInstanceOf[Iterable[(Int, Int)]])
    }.toVector

  private def encodeStrings(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    partitions.map { partition =>
      BatchCodec.strings(partition.data.asInstanceOf[Iterable[String]])
    }.toVector

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def stringResult(value: Any): String = value match
    case null => null
    case text: String => text
    case other =>
      throw new ClassCastException(
        s"columnar string map expected a String, found ${other.getClass.getName}",
      )

  private def decode(columns: Columns): Seq[Partition[_]] = columns match
    case Columns.Ints(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeInts(batch)))
    case Columns.Pairs(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeIntPairs(batch)))
    case Columns.Triples(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeIntTriples(batch)))
    case Columns.Strings(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeStrings(batch)))

  private def narrow(distribution: Distribution): PartitioningInfo =
    PartitioningInfo(distribution, OrderingTag.Unsorted, layout)

  private def hashed: PartitioningInfo =
    PartitioningInfo(
      Distribution.Hash(shuffleWidth, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Unsorted,
      layout,
    )

  private def layout: Layout = Layout.Columnar(SparkletConf.get.columnarBatchSize)

  private def batchSize: Int =
    val size = SparkletConf.get.columnarBatchSize
    require(size > 0, s"columnar batch size must be positive, got $size")
    size

  private def parallelism: Int =
    val workers = SparkletConf.get.threadPoolSize
    require(workers > 0, s"thread pool size must be positive, got $workers")
    workers

  private def shuffleWidth: Int =
    val width = SparkletConf.get.defaultShufflePartitions
    require(width > 0, s"shuffle partitions must be positive, got $width")
    width
