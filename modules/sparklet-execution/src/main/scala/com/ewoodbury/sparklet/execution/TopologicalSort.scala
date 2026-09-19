package com.ewoodbury.sparklet.execution

import scala.collection.mutable

import com.ewoodbury.sparklet.core.StageId

/**
 * Utility class for performing topological sort on stage dependencies.
 */
@SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
object TopologicalSort:
  /**
   * Performs a topological sort over the given stages.
   *
   * @param stages
   *   All stage IDs in the graph; stages without dependency edges are included as-is
   * @param dependencies
   *   A map of stage IDs to the stage IDs they depend on
   * @return
   *   A list of all stage IDs in topological order
   * @throws IllegalStateException
   *   if a cycle is detected in the dependencies
   */
  def sort(stages: Set[StageId], dependencies: Map[StageId, Set[StageId]]): List[StageId] = {
    val inDegree = mutable.Map[StageId, Int]()

    for (stage <- stages) {
      inDegree(stage) = 0
    }

    for ((stage, deps) <- dependencies; _ <- deps) {
      inDegree(stage) = inDegree(stage) + 1
    }

    val queue = mutable.Queue[StageId]()
    for ((stage, degree) <- inDegree if degree == 0) {
      queue.enqueue(stage)
    }

    val result = mutable.ListBuffer[StageId]()

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      result += current
      for ((stage, deps) <- dependencies if deps.contains(current)) {
        inDegree(stage) = inDegree(stage) - 1
        if (inDegree(stage) == 0) queue.enqueue(stage)
      }
    }
    if (inDegree.sizeIs != result.size)
      throw new IllegalStateException("Cycle detected in stage dependencies")
    result.toList
  }
