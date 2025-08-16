package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.StageId

/**
 * Utility class for performing topological sort on stage dependencies.
 */
object TopologicalSort:
  /**
   * Performs a topological sort on the stage dependencies.
   *
   * @param dependencies
   *   A map of stage IDs to their dependent stage IDs
   * @return
   *   A list of stage IDs in topological order
   * @throws IllegalStateException
   *   if a cycle is detected in the dependencies
   */
  def sort(dependencies: Map[StageId, Set[StageId]]): List[StageId] = {
    val allStages = dependencies.keys.toSet ++ dependencies.values.flatten

    def calculateInDegree(stages: Set[StageId]): Map[StageId, Int] = {
      val initialDegrees = stages.map(_ -> 0).toMap
      dependencies.foldLeft(initialDegrees) { case (degrees, (stage, deps)) =>
        deps.foldLeft(degrees) { (acc, dep) =>
          acc.updated(stage, acc(stage) + 1)
        }
      }
    }

    @annotation.tailrec
    def topologicalSortRec(
        remaining: Map[StageId, Int],
        result: List[StageId],
    ): List[StageId] = {
      val zeroInDegree = remaining.filter(_._2 == 0).keys.toSet

      if (zeroInDegree.isEmpty) {
        if (remaining.nonEmpty) {
          throw new IllegalStateException("Cycle detected in stage dependencies")
        } else {
          result.reverse
        }
      } else {
        val current = zeroInDegree.headOption.getOrElse(
          throw new IllegalStateException("No stage with zero in-degree found"),
        )
        val newRemaining = remaining - current
        val updatedRemaining = dependencies
          .getOrElse(current, Set.empty)
          .foldLeft(newRemaining) { (acc, dependent) =>
            acc.get(dependent) match {
              case Some(degree) => acc.updated(dependent, degree - 1)
              case None => acc
            }
          }

        topologicalSortRec(updatedRemaining, current :: result)
      }
    }

    val initialInDegree = calculateInDegree(allStages)
    topologicalSortRec(initialInDegree, List.empty)
  }
