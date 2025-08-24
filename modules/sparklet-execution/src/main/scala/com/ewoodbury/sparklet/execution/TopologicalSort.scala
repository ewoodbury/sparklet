package com.ewoodbury.sparklet.execution

import scala.collection.mutable

import com.ewoodbury.sparklet.core.StageId

/**
 * Utility class for performing topological sort on stage dependencies.
 */
@SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
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
    val inDegree = mutable.Map[StageId, Int]()
    val allStages = dependencies.keys.toSet ++ dependencies.values.flatten

    for (stage <- allStages) {
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
    if (allStages.sizeIs != result.size)
      throw new IllegalStateException("Cycle detected in stage dependencies")
    result.toList
  }
