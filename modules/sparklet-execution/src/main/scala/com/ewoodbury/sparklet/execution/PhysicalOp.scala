package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.Partition

/**
 * One columnar operator, lowered from a logical [[com.ewoodbury.sparklet.core.Plan]].
 *
 * User functions stay on the logical plan, so lowering and interpretation have to agree node for
 * node. A filter and project chain is one node per step. [[PhysicalOp.Project]] keeps the child
 * kind, and lowering builds it only for an int or a string column. [[PhysicalOp.Exchange]] sits
 * under [[PhysicalOp.HashAggregate]] and on each side of [[PhysicalOp.HashJoin]]. Those kernels
 * perform the shuffle. Interpreting the boundary on its own does not.
 */
enum PhysicalOp:
  case Scan(sourceKind: PhysicalOp.Kind, partitions: Seq[Partition[_]])
  case Filter(child: PhysicalOp, slot: PhysicalOp.Slot)
  case Project(child: PhysicalOp)
  case Exchange(child: PhysicalOp)
  case HashAggregate(child: PhysicalOp)
  case HashJoin(left: PhysicalOp, right: PhysicalOp)

  def kind: PhysicalOp.Kind = this match
    case Scan(sourceKind, _) => sourceKind
    case Filter(child, _) => child.kind
    case Project(child) => child.kind
    case Exchange(child) => child.kind
    case HashAggregate(_) => PhysicalOp.Kind.Pairs
    case HashJoin(_, _) => PhysicalOp.Kind.Triples

  /** Operator nesting, without data. */
  def shape: String = this match
    case Scan(_, _) => "Scan"
    case Filter(child, slot) => s"${slot.label}(${child.shape})"
    case Project(child) => s"Project(${child.shape})"
    case Exchange(child) => s"Exchange(${child.shape})"
    case HashAggregate(child) => s"HashAggregate(${child.shape})"
    case HashJoin(left, right) => s"HashJoin(${left.shape}, ${right.shape})"

object PhysicalOp:
  enum Kind:
    case Ints, Pairs, Triples, Strings

  /**
   * `Element` is the single column, an int or a string, and its shape label is `Filter`. `Row` is
   * a predicate on both columns of a pair. `Key` and `Value` name which pair column.
   */
  enum Slot:
    case Element, Row, Key, Value

    def label: String = this match
      case Element => "Filter"
      case Row => "FilterRow"
      case Key => "FilterKey"
      case Value => "FilterValue"
