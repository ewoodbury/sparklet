package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.columnar.*
import com.ewoodbury.sparklet.core.{Partition, Plan, SparkletConf}

/**
 * Runs `Int` and `(Int, Int)` plans on the columnar kernels.
 *
 * Named erasure boundary. `Plan` is invariant and its element type is erased, so a source is
 * classified by reading one value, and user functions are then cast to `Int` operations. A
 * function that does not return `Int` throws `ClassCastException`. [[tryRun]] catches that and
 * returns `None`, and the row path runs the whole plan. Any unsupported node (union, flatMap,
 * distinct, sort, a map that is not fed by `Int`, and the rest) makes the whole plan ineligible,
 * so a supported prefix is not executed on its own.
 *
 * Narrow map and filter keep input order and the input partition count. `reduceByKey` applies the
 * user's function and wraps at `Int`. `join` hash-partitions both sides and probes one bucket at a
 * time. Output width for those two is `defaultShufflePartitions`.
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
  def partitioning[A](plan: Plan[A]): Option[PartitioningInfo] = selected(plan)

  /**
   * Run `plan` on the columnar path. `None` means the caller should use the row path. The single
   * result cast is safe when selection succeeded: the decoded partitions have the plan's element
   * type. A `ClassCastException` from a function that is not actually `Int => Int` is a row-path
   * fallback, and user side effects in that function may already have run.
   */
  def tryRun[A](plan: Plan[A]): Option[Seq[Partition[A]]] =
    if (selected(plan).isEmpty) then None
    else
      try Some(decode(eval(plan)).asInstanceOf[Seq[Partition[A]]])
      catch case _: ClassCastException => None

  private enum SourceKind:
    case Ints, Pairs

  private enum OutputKind:
    case Ints, Pairs, Triples

  private enum Columns:
    case Ints(parts: Vector[ColumnBatch])
    case Pairs(parts: Vector[ColumnBatch])
    case Triples(parts: Vector[ColumnBatch])

  private enum Route:
    case Skip
    case Run(info: PartitioningInfo, encoded: OutputKind)

  private def selected(plan: Plan[_]): Option[PartitioningInfo] =
    if (!SparkletConf.get.columnarExecution) then None
    else
      describe(plan) match
        case None => None
        case Some(info) if info.invalidReason.isDefined => None
        case Some(info) => Some(info)

  private def describe(plan: Plan[_]): Option[PartitioningInfo] =
    route(plan) match
      case Route.Skip => None
      case Route.Run(info, _) => Some(info)

  private def route(plan: Plan[_]): Route = plan match
    case Plan.Source(partitions) =>
      encodedSource(partitions) match
        case Some(SourceKind.Ints) =>
          Route.Run(narrow(Distribution.Unknown(partitions.length)), OutputKind.Ints)
        case Some(SourceKind.Pairs) =>
          Route.Run(narrow(Distribution.Unknown(partitions.length)), OutputKind.Pairs)
        case None => Route.Skip
    case Plan.FilterOp(source, _) =>
      route(source) match
        case Route.Run(info, OutputKind.Ints) => Route.Run(info, OutputKind.Ints)
        case Route.Run(info, OutputKind.Pairs) => Route.Run(info, OutputKind.Pairs)
        case _ => Route.Skip
    case Plan.MapOp(source, _) =>
      route(source) match
        case Route.Run(info, OutputKind.Ints) => Route.Run(info, OutputKind.Ints)
        case _ => Route.Skip
    case Plan.FilterKeysOp(source, _) =>
      keepPairs(route(source))
    case Plan.FilterValuesOp(source, _) =>
      keepPairs(route(source))
    case Plan.ReduceByKeyOp(source, _) =>
      route(source) match
        case Route.Run(_, OutputKind.Pairs) => Route.Run(hashed, OutputKind.Pairs)
        case _ => Route.Skip
    case Plan.JoinOp(left, right, _) =>
      (route(left), route(right)) match
        case (Route.Run(_, OutputKind.Pairs), Route.Run(_, OutputKind.Pairs)) =>
          Route.Run(hashed, OutputKind.Triples)
        case _ => Route.Skip
    case _ => Route.Skip

  private def keepPairs(parent: Route): Route = parent match
    case Route.Run(info, OutputKind.Pairs) => Route.Run(info, OutputKind.Pairs)
    case _ => Route.Skip

  private def narrow(distribution: Distribution): PartitioningInfo =
    PartitioningInfo(distribution, OrderingTag.Unsorted, layout)

  private def hashed: PartitioningInfo =
    PartitioningInfo(
      Distribution.Hash(shuffleWidth, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Unsorted,
      layout,
    )

  private def layout: Layout = Layout.Columnar(SparkletConf.get.columnarBatchSize)

  private def encodedSource(partitions: Seq[Partition[_]]): Option[SourceKind] =
    if (partitions.isEmpty) then None
    else
      partitions.iterator.map(_.data.iterator).flatten.nextOption() match
        case Some(_: Int) => Some(SourceKind.Ints)
        case Some((_: Int, _: Int)) => Some(SourceKind.Pairs)
        case _ => None

  private def eval(plan: Plan[_]): Columns = plan match
    case Plan.Source(partitions) =>
      encodedSource(partitions) match
        case Some(SourceKind.Ints) => Columns.Ints(encodeInts(partitions))
        case Some(SourceKind.Pairs) => Columns.Pairs(encodePairs(partitions))
        case None => throw new IllegalStateException("columnar source has no int values")
    case Plan.FilterOp(source, predicate) =>
      eval(source) match
        case Columns.Ints(parts) =>
          val keep = predicate.asInstanceOf[Int => Boolean]
          Columns.Ints(onEach(parts) { batch =>
            ColumnKernel.filterInt32(batch, 0, value => keep(value))
          })
        case Columns.Pairs(parts) =>
          val keep = predicate.asInstanceOf[((Int, Int)) => Boolean]
          Columns.Pairs(onEach(parts) { batch =>
            PairKernel.filter(batch, (key, value) => keep((key, value)))
          })
        case Columns.Triples(_) =>
          throw new IllegalStateException("columnar filter does not accept a join result")
    case Plan.MapOp(source, mapper) =>
      val fn = mapper.asInstanceOf[Int => Int]
      eval(source) match
        case Columns.Ints(parts) =>
          Columns.Ints(onEach(parts) { batch =>
            ColumnKernel.projectInt32(batch, 0, value => fn(value))
          })
        case _ =>
          throw new IllegalStateException("columnar map expects ints")
    case Plan.FilterKeysOp(source, predicate) =>
      val keep = predicate.asInstanceOf[Int => Boolean]
      Columns.Pairs(onEach(pairsOf(eval(source))) { batch =>
        ColumnKernel.filterInt32(batch, 0, value => keep(value))
      })
    case Plan.FilterValuesOp(source, predicate) =>
      val keep = predicate.asInstanceOf[Int => Boolean]
      Columns.Pairs(onEach(pairsOf(eval(source))) { batch =>
        ColumnKernel.filterInt32(batch, 1, value => keep(value))
      })
    case Plan.ReduceByKeyOp(source, reduceFunc) =>
      val op = reduceFunc.asInstanceOf[(Int, Int) => Int]
      val reduced = HashReduce.byKey(
        pairsOf(eval(source)),
        shuffleWidth,
        parallelism,
        batchSize,
        (left, right) => op(left, right),
      )
      Columns.Pairs(reduced)
    case Plan.JoinOp(left, right, _) =>
      val joined = ColumnPipeline.innerJoin(
        pairsOf(eval(left)),
        pairsOf(eval(right)),
        shuffleWidth,
        parallelism,
        batchSize,
      )
      Columns.Triples(joined)
    case _ =>
      throw new IllegalStateException(
        s"columnar planner cannot run ${plan.getClass.getSimpleName}",
      )

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
      BatchCodec.ints(partition.data.asInstanceOf[Iterable[Int]].toSeq)
    }.toVector

  private def encodePairs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    partitions.map { partition =>
      BatchCodec.intPairs(partition.data.asInstanceOf[Iterable[(Int, Int)]].toSeq)
    }.toVector

  private def decode(columns: Columns): Seq[Partition[_]] = columns match
    case Columns.Ints(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeInts(batch)))
    case Columns.Pairs(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeIntPairs(batch)))
    case Columns.Triples(parts) =>
      parts.map(batch => Partition(decodeTriples(batch)))

  private def decodeTriples(batch: ColumnBatch): Seq[(Int, (Int, Int))] =
    (batch.columns.lift(0), batch.columns.lift(1), batch.columns.lift(2)) match
      case (Some(keys: Int32Column), Some(build: Int32Column), Some(probe: Int32Column)) =>
        Seq.tabulate(batch.length) { row =>
          require(
            keys.isValid(row) && build.isValid(row) && probe.isValid(row),
            s"null join value at row $row",
          )
          (keys.values(row), (build.values(row), probe.values(row)))
        }
      case _ =>
        throw new IllegalArgumentException(
          s"join output is not three Int32 columns: ${batch.schema}",
        )

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
