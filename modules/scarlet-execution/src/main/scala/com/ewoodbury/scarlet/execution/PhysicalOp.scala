package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Partition

/**
 * One columnar operator, lowered from a logical [[com.ewoodbury.scarlet.core.Plan]].
 *
 * User functions stay on the logical plan, so lowering and interpretation have to agree node for
 * node. A straight int, float, or string filter/project chain can be one [[PhysicalOp.Pipeline]]
 * node. Other filters and projects remain one node per step. [[PhysicalOp.Project]] and `Pipeline`
 * keep the child kind. [[PhysicalOp.Exchange]] sits under [[PhysicalOp.HashAggregate]] and on each
 * side of [[PhysicalOp.HashJoin]]. Those kernels perform the shuffle. Interpreting the boundary on
 * its own does not.
 */
enum PhysicalOp:
  case Scan(sourceKind: PhysicalOp.Kind, partitions: Seq[Partition[_]])
  case Filter(child: PhysicalOp, slot: PhysicalOp.Slot)
  case Project(child: PhysicalOp)

  /**
   * An element pipeline for the outer `length` consecutive logical FilterOp/MapOp nodes.
   * Interpretation re-derives their kinds and reads their functions from Plan.
   */
  case Pipeline(child: PhysicalOp, length: Int)
  case Exchange(child: PhysicalOp)
  case HashAggregate(child: PhysicalOp)
  case HashJoin(left: PhysicalOp, right: PhysicalOp)

  def kind: PhysicalOp.Kind = this match
    case Scan(sourceKind, _) => sourceKind
    case Filter(child, _) => child.kind
    case Project(child) => child.kind
    case Pipeline(child, _) => child.kind
    case Exchange(child) => child.kind
    case HashAggregate(_) => PhysicalOp.Kind.Pairs
    case HashJoin(_, _) => PhysicalOp.Kind.Triples

  /** Operator nesting, without data. */
  def shape: String = this match
    case Scan(_, _) => "Scan"
    case Filter(child, slot) => s"${slot.label}(${child.shape})"
    case Project(child) => s"Project(${child.shape})"
    case Pipeline(child, length) => s"Pipeline$length(${child.shape})"
    case Exchange(child) => s"Exchange(${child.shape})"
    case HashAggregate(child) => s"HashAggregate(${child.shape})"
    case HashJoin(left, right) => s"HashJoin(${left.shape}, ${right.shape})"

object PhysicalOp:
  enum Kind:
    case Ints, Floats, Pairs, Triples, Strings

  /**
   * `Element` is the single column, an int, a double, or a string, and its shape label is
   * `Filter`. `Row` is a predicate on both columns of a pair. `Key` and `Value` name which pair
   * column.
   */
  enum Slot:
    case Element, Row, Key, Value

    def label: String = this match
      case Element => "Filter"
      case Row => "FilterRow"
      case Key => "FilterKey"
      case Value => "FilterValue"
