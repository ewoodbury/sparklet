package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}

/**
 * Lowers `Int`, `Double`, `(Int, Int)`, and `String` plans to [[PhysicalOp]] and runs that tree on
 * the columnar kernels.
 *
 * Named erasure boundary. `Plan` is invariant and its element type is erased, so a source is
 * classified by reading one value, and user functions are then cast to that operation. A function
 * that does not return the selected type throws `ClassCastException`. [[tryRun]] catches that and
 * returns `None`, and the row path runs the whole plan. Any unsupported node (union, flatMap,
 * distinct, sort, a map that is not fed by `Int`, `Double`, or `String`, and the rest) makes the
 * whole plan ineligible, so a supported prefix is not executed on its own.
 *
 * [[lower]] and interpretation walk the same logical plan. The physical node records the operator
 * and not the function, so each logical node is matched to the node [[lower]] built for it. A
 * mismatch is a planner bug: interpretation throws `IllegalStateException`, and [[tryRun]] does
 * not turn that into a row-path fallback. A `ClassCastException` from a function that is not the
 * selected type still is a fallback, and user side effects in that function may already have run.
 *
 * [[lower]] does not call user functions. A straight `Int`, `Double`, or `String` filter/project
 * run of two to four steps is one [[PhysicalOp.Pipeline]]. A longer run is cut into those chunks,
 * source order first. One step stays a [[PhysicalOp.Filter]] or [[PhysicalOp.Project]]. Pair
 * filters, aggregates, and joins stay one node per operator. Narrow map and filter keep input
 * order and the input partition count. `Double` and `String` select filter and map only. A filter
 * over a join result is not columnar. `reduceByKey` applies the user's function and wraps at
 * `Int`. `join` hash-partitions both sides and probes one bucket at a time. Output width for those
 * two is `defaultShufflePartitions`.
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
    if (!ScarletConf.get.columnarExecution) then None
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
    case Floats(parts: Vector[ColumnBatch])
    case Pairs(parts: Vector[ColumnBatch])
    case Triples(parts: Vector[ColumnBatch])
    case Strings(parts: Vector[ColumnBatch])

  private enum ElementStep:
    case Filter(predicate: Any)
    case Project(mapper: Any)

  private def accepted(plan: Plan[_]): Option[PhysicalOp] =
    lower(plan).filter(op => infoOf(op).invalidReason.isEmpty)

  private def lowerPlan(plan: Plan[_]): Option[PhysicalOp] =
    val (base, steps) = elementSteps(plan, Int.MaxValue)
    if (steps.isEmpty) then lowerPlanNode(base)
    else lowerPlan(base).flatMap(child => lowerElementSteps(child, steps))

  private def lowerPlanNode(plan: Plan[_]): Option[PhysicalOp] = plan match
    case Plan.Source(partitions) =>
      classify(partitions).map(kind => PhysicalOp.Scan(kind, partitions))
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

  private def elementSteps(
      plan: Plan[_],
      limit: Int,
  ): (Plan[_], List[ElementStep]) =
    @tailrec
    def collect(
        current: Plan[_],
        remaining: Int,
        steps: List[ElementStep],
    ): (Plan[_], List[ElementStep]) =
      if (remaining == 0) then (current, steps)
      else
        current match
          case Plan.FilterOp(source, predicate) =>
            collect(source, remaining - 1, ElementStep.Filter(predicate) :: steps)
          case Plan.MapOp(source, mapper) =>
            collect(source, remaining - 1, ElementStep.Project(mapper) :: steps)
          case _ => (current, steps)

    collect(plan, limit, Nil)

  private def lowerElementSteps(
      child: PhysicalOp,
      steps: List[ElementStep],
  ): Option[PhysicalOp] = child.kind match
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Floats | PhysicalOp.Kind.Strings =>
      Some(lowerElementPipeline(child, steps))
    case _ =>
      steps.foldLeft(Option(child)) { (current, step) =>
        current.flatMap(op => lowerElementStep(op, step))
      }

  private def lowerElementStep(child: PhysicalOp, step: ElementStep): Option[PhysicalOp] =
    step match
      case ElementStep.Filter(_) =>
        child.kind match
          case PhysicalOp.Kind.Pairs => Some(PhysicalOp.Filter(child, PhysicalOp.Slot.Row))
          case _ => None
      case ElementStep.Project(_) => None

  private def lowerElementPipeline(child: PhysicalOp, steps: List[ElementStep]): PhysicalOp =
    steps.grouped(4).foldLeft(child) { (current, group) =>
      group match
        case List(step) =>
          step match
            case ElementStep.Filter(_) => PhysicalOp.Filter(current, PhysicalOp.Slot.Element)
            case ElementStep.Project(_) => PhysicalOp.Project(current)
        case _ => PhysicalOp.Pipeline(current, group.length)
    }

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
      case Some(_: Double) => Some(PhysicalOp.Kind.Floats)
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
    case PhysicalOp.Pipeline(child, _) => infoOf(child)
    case _: PhysicalOp.HashAggregate | _: PhysicalOp.HashJoin => hashed
    case PhysicalOp.Exchange(_) =>
      throw new IllegalStateException("exchange is only a child of aggregate or join")
    case PhysicalOp.Scan(_, partitions) =>
      narrow(Distribution.Unknown(partitions.length))

  private def interpret(plan: Plan[_], op: PhysicalOp): Columns = (plan, op) match
    case (Plan.Source(partitions), PhysicalOp.Scan(PhysicalOp.Kind.Ints, _)) =>
      Columns.Ints(encodeInts(partitions))
    case (Plan.Source(partitions), PhysicalOp.Scan(PhysicalOp.Kind.Floats, _)) =>
      Columns.Floats(encodeFloats(partitions))
    case (Plan.Source(partitions), PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) =>
      Columns.Pairs(encodePairs(partitions))
    case (Plan.Source(partitions), PhysicalOp.Scan(PhysicalOp.Kind.Strings, _)) =>
      Columns.Strings(encodeStrings(partitions))
    case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, slot)) =>
      applyFilter(interpret(source, child), predicate, slot)
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child)) =>
      project(interpret(source, child), mapper)
    case (logical, PhysicalOp.Pipeline(child, length)) =>
      interpretPipeline(logical, child, length)
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

  private def interpretPipeline(plan: Plan[_], child: PhysicalOp, length: Int): Columns =
    val (source, steps) = elementSteps(plan, length)
    if (steps.length < length) then
      // Lowering uses this same collector, so this only guards a malformed physical tree.
      throw new IllegalStateException("pipeline does not match logical element steps")
    pipeline(interpret(source, child), steps)

  private def pipeline(columns: Columns, steps: List[ElementStep]): Columns = columns match
    case Columns.Ints(parts) =>
      Columns.Ints(onEach(parts)(batch => runIntPipeline(batch, steps)))
    case Columns.Floats(parts) =>
      Columns.Floats(onEach(parts)(batch => runFloatPipeline(batch, steps)))
    case Columns.Strings(parts) =>
      Columns.Strings(onEach(parts)(batch => runStringPipeline(batch, steps)))
    case _ => throw new IllegalStateException("pipeline expects an int, float, or string column")

  private def runIntPipeline(batch: ColumnBatch, steps: List[ElementStep]): ColumnBatch =
    import ElementStep.*
    steps match
      case List(Filter(f1), Filter(f2)) =>
        ColumnKernel.pipelineInt32FF(batch, intPredicate(f1), intPredicate(f2))
      case List(Project(m1), Filter(f2)) =>
        ColumnKernel.pipelineInt32PF(batch, intMap(m1), intPredicate(f2))
      case List(Filter(f1), Project(m2)) =>
        ColumnKernel.pipelineInt32FP(batch, intPredicate(f1), intMap(m2))
      case List(Project(m1), Project(m2)) =>
        ColumnKernel.pipelineInt32PP(batch, intMap(m1), intMap(m2))
      case List(Filter(f1), Filter(f2), Filter(f3)) =>
        ColumnKernel.pipelineInt32FFF(batch, intPredicate(f1), intPredicate(f2), intPredicate(f3))
      case List(Project(m1), Filter(f2), Filter(f3)) =>
        ColumnKernel.pipelineInt32PFF(batch, intMap(m1), intPredicate(f2), intPredicate(f3))
      case List(Filter(f1), Project(m2), Filter(f3)) =>
        ColumnKernel.pipelineInt32FPF(batch, intPredicate(f1), intMap(m2), intPredicate(f3))
      case List(Project(m1), Project(m2), Filter(f3)) =>
        ColumnKernel.pipelineInt32PPF(batch, intMap(m1), intMap(m2), intPredicate(f3))
      case List(Filter(f1), Filter(f2), Project(m3)) =>
        ColumnKernel.pipelineInt32FFP(batch, intPredicate(f1), intPredicate(f2), intMap(m3))
      case List(Project(m1), Filter(f2), Project(m3)) =>
        ColumnKernel.pipelineInt32PFP(batch, intMap(m1), intPredicate(f2), intMap(m3))
      case List(Filter(f1), Project(m2), Project(m3)) =>
        ColumnKernel.pipelineInt32FPP(batch, intPredicate(f1), intMap(m2), intMap(m3))
      case List(Project(m1), Project(m2), Project(m3)) =>
        ColumnKernel.pipelineInt32PPP(batch, intMap(m1), intMap(m2), intMap(m3))
      case List(Filter(f1), Filter(f2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineInt32FFFF(
          batch,
          intPredicate(f1),
          intPredicate(f2),
          intPredicate(f3),
          intPredicate(f4),
        )
      case List(Project(m1), Filter(f2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineInt32PFFF(
          batch,
          intMap(m1),
          intPredicate(f2),
          intPredicate(f3),
          intPredicate(f4),
        )
      case List(Filter(f1), Project(m2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineInt32FPFF(
          batch,
          intPredicate(f1),
          intMap(m2),
          intPredicate(f3),
          intPredicate(f4),
        )
      case List(Project(m1), Project(m2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineInt32PPFF(
          batch,
          intMap(m1),
          intMap(m2),
          intPredicate(f3),
          intPredicate(f4),
        )
      case List(Filter(f1), Filter(f2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineInt32FFPF(
          batch,
          intPredicate(f1),
          intPredicate(f2),
          intMap(m3),
          intPredicate(f4),
        )
      case List(Project(m1), Filter(f2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineInt32PFPF(
          batch,
          intMap(m1),
          intPredicate(f2),
          intMap(m3),
          intPredicate(f4),
        )
      case List(Filter(f1), Project(m2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineInt32FPPF(
          batch,
          intPredicate(f1),
          intMap(m2),
          intMap(m3),
          intPredicate(f4),
        )
      case List(Project(m1), Project(m2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineInt32PPPF(batch, intMap(m1), intMap(m2), intMap(m3), intPredicate(f4))
      case List(Filter(f1), Filter(f2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineInt32FFFP(
          batch,
          intPredicate(f1),
          intPredicate(f2),
          intPredicate(f3),
          intMap(m4),
        )
      case List(Project(m1), Filter(f2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineInt32PFFP(
          batch,
          intMap(m1),
          intPredicate(f2),
          intPredicate(f3),
          intMap(m4),
        )
      case List(Filter(f1), Project(m2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineInt32FPFP(
          batch,
          intPredicate(f1),
          intMap(m2),
          intPredicate(f3),
          intMap(m4),
        )
      case List(Project(m1), Project(m2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineInt32PPFP(batch, intMap(m1), intMap(m2), intPredicate(f3), intMap(m4))
      case List(Filter(f1), Filter(f2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineInt32FFPP(
          batch,
          intPredicate(f1),
          intPredicate(f2),
          intMap(m3),
          intMap(m4),
        )
      case List(Project(m1), Filter(f2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineInt32PFPP(batch, intMap(m1), intPredicate(f2), intMap(m3), intMap(m4))
      case List(Filter(f1), Project(m2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineInt32FPPP(batch, intPredicate(f1), intMap(m2), intMap(m3), intMap(m4))
      case List(Project(m1), Project(m2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineInt32PPPP(batch, intMap(m1), intMap(m2), intMap(m3), intMap(m4))
      case _ =>
        throw new IllegalStateException(
          s"unsupported int pipeline pattern of length ${steps.length}",
        )

  private def runFloatPipeline(batch: ColumnBatch, steps: List[ElementStep]): ColumnBatch =
    import ElementStep.*
    steps match
      case List(Filter(f1), Filter(f2)) =>
        ColumnKernel.pipelineFloat64FF(batch, floatPredicate(f1), floatPredicate(f2))
      case List(Project(m1), Filter(f2)) =>
        ColumnKernel.pipelineFloat64PF(batch, floatMap(m1), floatPredicate(f2))
      case List(Filter(f1), Project(m2)) =>
        ColumnKernel.pipelineFloat64FP(batch, floatPredicate(f1), floatMap(m2))
      case List(Project(m1), Project(m2)) =>
        ColumnKernel.pipelineFloat64PP(batch, floatMap(m1), floatMap(m2))
      case List(Filter(f1), Filter(f2), Filter(f3)) =>
        ColumnKernel.pipelineFloat64FFF(
          batch,
          floatPredicate(f1),
          floatPredicate(f2),
          floatPredicate(f3),
        )
      case List(Project(m1), Filter(f2), Filter(f3)) =>
        ColumnKernel.pipelineFloat64PFF(
          batch,
          floatMap(m1),
          floatPredicate(f2),
          floatPredicate(f3),
        )
      case List(Filter(f1), Project(m2), Filter(f3)) =>
        ColumnKernel.pipelineFloat64FPF(
          batch,
          floatPredicate(f1),
          floatMap(m2),
          floatPredicate(f3),
        )
      case List(Project(m1), Project(m2), Filter(f3)) =>
        ColumnKernel.pipelineFloat64PPF(batch, floatMap(m1), floatMap(m2), floatPredicate(f3))
      case List(Filter(f1), Filter(f2), Project(m3)) =>
        ColumnKernel.pipelineFloat64FFP(
          batch,
          floatPredicate(f1),
          floatPredicate(f2),
          floatMap(m3),
        )
      case List(Project(m1), Filter(f2), Project(m3)) =>
        ColumnKernel.pipelineFloat64PFP(batch, floatMap(m1), floatPredicate(f2), floatMap(m3))
      case List(Filter(f1), Project(m2), Project(m3)) =>
        ColumnKernel.pipelineFloat64FPP(batch, floatPredicate(f1), floatMap(m2), floatMap(m3))
      case List(Project(m1), Project(m2), Project(m3)) =>
        ColumnKernel.pipelineFloat64PPP(batch, floatMap(m1), floatMap(m2), floatMap(m3))
      case List(Filter(f1), Filter(f2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineFloat64FFFF(
          batch,
          floatPredicate(f1),
          floatPredicate(f2),
          floatPredicate(f3),
          floatPredicate(f4),
        )
      case List(Project(m1), Filter(f2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineFloat64PFFF(
          batch,
          floatMap(m1),
          floatPredicate(f2),
          floatPredicate(f3),
          floatPredicate(f4),
        )
      case List(Filter(f1), Project(m2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineFloat64FPFF(
          batch,
          floatPredicate(f1),
          floatMap(m2),
          floatPredicate(f3),
          floatPredicate(f4),
        )
      case List(Project(m1), Project(m2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineFloat64PPFF(
          batch,
          floatMap(m1),
          floatMap(m2),
          floatPredicate(f3),
          floatPredicate(f4),
        )
      case List(Filter(f1), Filter(f2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineFloat64FFPF(
          batch,
          floatPredicate(f1),
          floatPredicate(f2),
          floatMap(m3),
          floatPredicate(f4),
        )
      case List(Project(m1), Filter(f2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineFloat64PFPF(
          batch,
          floatMap(m1),
          floatPredicate(f2),
          floatMap(m3),
          floatPredicate(f4),
        )
      case List(Filter(f1), Project(m2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineFloat64FPPF(
          batch,
          floatPredicate(f1),
          floatMap(m2),
          floatMap(m3),
          floatPredicate(f4),
        )
      case List(Project(m1), Project(m2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineFloat64PPPF(
          batch,
          floatMap(m1),
          floatMap(m2),
          floatMap(m3),
          floatPredicate(f4),
        )
      case List(Filter(f1), Filter(f2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineFloat64FFFP(
          batch,
          floatPredicate(f1),
          floatPredicate(f2),
          floatPredicate(f3),
          floatMap(m4),
        )
      case List(Project(m1), Filter(f2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineFloat64PFFP(
          batch,
          floatMap(m1),
          floatPredicate(f2),
          floatPredicate(f3),
          floatMap(m4),
        )
      case List(Filter(f1), Project(m2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineFloat64FPFP(
          batch,
          floatPredicate(f1),
          floatMap(m2),
          floatPredicate(f3),
          floatMap(m4),
        )
      case List(Project(m1), Project(m2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineFloat64PPFP(
          batch,
          floatMap(m1),
          floatMap(m2),
          floatPredicate(f3),
          floatMap(m4),
        )
      case List(Filter(f1), Filter(f2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineFloat64FFPP(
          batch,
          floatPredicate(f1),
          floatPredicate(f2),
          floatMap(m3),
          floatMap(m4),
        )
      case List(Project(m1), Filter(f2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineFloat64PFPP(
          batch,
          floatMap(m1),
          floatPredicate(f2),
          floatMap(m3),
          floatMap(m4),
        )
      case List(Filter(f1), Project(m2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineFloat64FPPP(
          batch,
          floatPredicate(f1),
          floatMap(m2),
          floatMap(m3),
          floatMap(m4),
        )
      case List(Project(m1), Project(m2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineFloat64PPPP(
          batch,
          floatMap(m1),
          floatMap(m2),
          floatMap(m3),
          floatMap(m4),
        )
      case _ =>
        throw new IllegalStateException(
          s"unsupported float pipeline pattern of length ${steps.length}",
        )

  private def runStringPipeline(batch: ColumnBatch, steps: List[ElementStep]): ColumnBatch =
    import ElementStep.*
    steps match
      case List(Filter(f1), Filter(f2)) =>
        ColumnKernel.pipelineUtf8FF(batch, stringPredicate(f1), stringPredicate(f2))
      case List(Project(m1), Filter(f2)) =>
        ColumnKernel.pipelineUtf8PF(batch, stringMap(m1), stringPredicate(f2))
      case List(Filter(f1), Project(m2)) =>
        ColumnKernel.pipelineUtf8FP(batch, stringPredicate(f1), stringMap(m2))
      case List(Project(m1), Project(m2)) =>
        ColumnKernel.pipelineUtf8PP(batch, stringMap(m1), stringMap(m2))
      case List(Filter(f1), Filter(f2), Filter(f3)) =>
        ColumnKernel.pipelineUtf8FFF(
          batch,
          stringPredicate(f1),
          stringPredicate(f2),
          stringPredicate(f3),
        )
      case List(Project(m1), Filter(f2), Filter(f3)) =>
        ColumnKernel.pipelineUtf8PFF(
          batch,
          stringMap(m1),
          stringPredicate(f2),
          stringPredicate(f3),
        )
      case List(Filter(f1), Project(m2), Filter(f3)) =>
        ColumnKernel.pipelineUtf8FPF(
          batch,
          stringPredicate(f1),
          stringMap(m2),
          stringPredicate(f3),
        )
      case List(Project(m1), Project(m2), Filter(f3)) =>
        ColumnKernel.pipelineUtf8PPF(batch, stringMap(m1), stringMap(m2), stringPredicate(f3))
      case List(Filter(f1), Filter(f2), Project(m3)) =>
        ColumnKernel.pipelineUtf8FFP(
          batch,
          stringPredicate(f1),
          stringPredicate(f2),
          stringMap(m3),
        )
      case List(Project(m1), Filter(f2), Project(m3)) =>
        ColumnKernel.pipelineUtf8PFP(batch, stringMap(m1), stringPredicate(f2), stringMap(m3))
      case List(Filter(f1), Project(m2), Project(m3)) =>
        ColumnKernel.pipelineUtf8FPP(batch, stringPredicate(f1), stringMap(m2), stringMap(m3))
      case List(Project(m1), Project(m2), Project(m3)) =>
        ColumnKernel.pipelineUtf8PPP(batch, stringMap(m1), stringMap(m2), stringMap(m3))
      case List(Filter(f1), Filter(f2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineUtf8FFFF(
          batch,
          stringPredicate(f1),
          stringPredicate(f2),
          stringPredicate(f3),
          stringPredicate(f4),
        )
      case List(Project(m1), Filter(f2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineUtf8PFFF(
          batch,
          stringMap(m1),
          stringPredicate(f2),
          stringPredicate(f3),
          stringPredicate(f4),
        )
      case List(Filter(f1), Project(m2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineUtf8FPFF(
          batch,
          stringPredicate(f1),
          stringMap(m2),
          stringPredicate(f3),
          stringPredicate(f4),
        )
      case List(Project(m1), Project(m2), Filter(f3), Filter(f4)) =>
        ColumnKernel.pipelineUtf8PPFF(
          batch,
          stringMap(m1),
          stringMap(m2),
          stringPredicate(f3),
          stringPredicate(f4),
        )
      case List(Filter(f1), Filter(f2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineUtf8FFPF(
          batch,
          stringPredicate(f1),
          stringPredicate(f2),
          stringMap(m3),
          stringPredicate(f4),
        )
      case List(Project(m1), Filter(f2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineUtf8PFPF(
          batch,
          stringMap(m1),
          stringPredicate(f2),
          stringMap(m3),
          stringPredicate(f4),
        )
      case List(Filter(f1), Project(m2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineUtf8FPPF(
          batch,
          stringPredicate(f1),
          stringMap(m2),
          stringMap(m3),
          stringPredicate(f4),
        )
      case List(Project(m1), Project(m2), Project(m3), Filter(f4)) =>
        ColumnKernel.pipelineUtf8PPPF(
          batch,
          stringMap(m1),
          stringMap(m2),
          stringMap(m3),
          stringPredicate(f4),
        )
      case List(Filter(f1), Filter(f2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineUtf8FFFP(
          batch,
          stringPredicate(f1),
          stringPredicate(f2),
          stringPredicate(f3),
          stringMap(m4),
        )
      case List(Project(m1), Filter(f2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineUtf8PFFP(
          batch,
          stringMap(m1),
          stringPredicate(f2),
          stringPredicate(f3),
          stringMap(m4),
        )
      case List(Filter(f1), Project(m2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineUtf8FPFP(
          batch,
          stringPredicate(f1),
          stringMap(m2),
          stringPredicate(f3),
          stringMap(m4),
        )
      case List(Project(m1), Project(m2), Filter(f3), Project(m4)) =>
        ColumnKernel.pipelineUtf8PPFP(
          batch,
          stringMap(m1),
          stringMap(m2),
          stringPredicate(f3),
          stringMap(m4),
        )
      case List(Filter(f1), Filter(f2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineUtf8FFPP(
          batch,
          stringPredicate(f1),
          stringPredicate(f2),
          stringMap(m3),
          stringMap(m4),
        )
      case List(Project(m1), Filter(f2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineUtf8PFPP(
          batch,
          stringMap(m1),
          stringPredicate(f2),
          stringMap(m3),
          stringMap(m4),
        )
      case List(Filter(f1), Project(m2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineUtf8FPPP(
          batch,
          stringPredicate(f1),
          stringMap(m2),
          stringMap(m3),
          stringMap(m4),
        )
      case List(Project(m1), Project(m2), Project(m3), Project(m4)) =>
        ColumnKernel.pipelineUtf8PPPP(
          batch,
          stringMap(m1),
          stringMap(m2),
          stringMap(m3),
          stringMap(m4),
        )
      case _ =>
        throw new IllegalStateException(
          s"unsupported string pipeline pattern of length ${steps.length}",
        )

  private def intPredicate(predicate: Any): ColumnKernel.Int32Predicate =
    val fn = predicate.asInstanceOf[Int => Boolean]
    value => fn(value)

  private def intMap(mapper: Any): ColumnKernel.Int32Map =
    val fn = mapper.asInstanceOf[Int => Int]
    value => fn(value)

  private def floatPredicate(predicate: Any): ColumnKernel.Float64Predicate =
    val fn = predicate.asInstanceOf[Double => Boolean]
    value => fn(value)

  private def floatMap(mapper: Any): ColumnKernel.Float64Map =
    val fn = mapper.asInstanceOf[Double => Double]
    value => fn(value)

  private def stringPredicate(predicate: Any): ColumnKernel.Utf8Predicate =
    val fn = predicate.asInstanceOf[String => Boolean]
    value => fn(value)

  private def stringMap(mapper: Any): ColumnKernel.Utf8Map =
    val fn = mapper.asInstanceOf[String => Any]
    value => stringResult(fn(value))

  private def applyFilter(columns: Columns, predicate: Any, slot: PhysicalOp.Slot): Columns =
    slot match
      case PhysicalOp.Slot.Element =>
        columns match
          case Columns.Ints(parts) =>
            val keep = predicate.asInstanceOf[Int => Boolean]
            Columns.Ints(onEach(parts) { batch =>
              ColumnKernel.filterInt32(batch, 0, value => keep(value))
            })
          case Columns.Floats(parts) =>
            val keep = predicate.asInstanceOf[Double => Boolean]
            Columns.Floats(onEach(parts) { batch =>
              ColumnKernel.filterFloat64(batch, 0, value => keep(value))
            })
          case Columns.Strings(parts) =>
            val keep = predicate.asInstanceOf[String => Boolean]
            Columns.Strings(onEach(parts) { batch =>
              ColumnKernel.filterUtf8(batch, 0, value => keep(value))
            })
          case _ =>
            throw new IllegalStateException(
              "columnar element filter expects ints, floats, or strings",
            )
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
    case Columns.Floats(parts) =>
      val fn = mapper.asInstanceOf[Double => Double]
      Columns.Floats(onEach(parts) { batch =>
        ColumnKernel.projectFloat64(batch, 0, value => fn(value))
      })
    case Columns.Strings(parts) =>
      val fn = mapper.asInstanceOf[String => Any]
      Columns.Strings(onEach(parts) { batch =>
        ColumnKernel.projectUtf8(batch, 0, value => stringResult(fn(value)))
      })
    case _ =>
      throw new IllegalStateException("columnar map expects ints, floats, or strings")

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

  private def encodeFloats(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    partitions.map { partition =>
      BatchCodec.float64s(partition.data.asInstanceOf[Iterable[Double]])
    }.toVector

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
    case Columns.Floats(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeFloat64s(batch)))
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

  private def layout: Layout = Layout.Columnar(ScarletConf.get.columnarBatchSize)

  private def batchSize: Int =
    val size = ScarletConf.get.columnarBatchSize
    require(size > 0, s"columnar batch size must be positive, got $size")
    size

  private def parallelism: Int =
    val workers = ScarletConf.get.threadPoolSize
    require(workers > 0, s"thread pool size must be positive, got $workers")
    workers

  private def shuffleWidth: Int =
    val width = ScarletConf.get.defaultShufflePartitions
    require(width > 0, s"shuffle partitions must be positive, got $width")
    width
