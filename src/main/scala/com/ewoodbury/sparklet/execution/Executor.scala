package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.{Partition, Plan}

@SuppressWarnings(Array("org.wartremover.warts.Any"))

object Executor:
  /**
   * Translates a Plan into a sequence of executable Tasks using stage-based execution. This groups
   * narrow transformations together into stages for efficient execution.
   */
  def createTasks[A](plan: Plan[A]): Seq[Task[_, A]] = {
    if (DAGScheduler.requiresDAGScheduling(plan)) {
      Seq(Task.DAGTask(plan))
    } else {
    val stages = StageBuilder.buildStages(plan)
    stages.flatMap { case (source, stage) =>
      source.partitions.map { partition =>
          Task.StageTask(partition.asInstanceOf[Partition[Any]], stage.asInstanceOf[Stage[Any, A]])
        }
      }
    }
  }
end Executor
