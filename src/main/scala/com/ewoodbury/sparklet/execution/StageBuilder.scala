package com.ewoodbury.sparklet.execution

import com.ewoodbury.sparklet.core.Plan

@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))

/**
 * Builds stages from plans by grouping narrow transformations together.
 */
object StageBuilder:
  
  /**
   * Converts a plan into a sequence of stages with their source partitions.
   * Returns (source_partitions, stage) pairs.
   */
  def buildStages[A](plan: Plan[A]): Seq[(Plan.Source[_], Stage[_, A])] = {
    buildStagesRecursive(plan)
  }
  
  private def buildStagesRecursive[A](plan: Plan[A]): Seq[(Plan.Source[_], Stage[_, A])] = {
    plan match {
      case source: Plan.Source[A] => 
        // Base case: source creates an identity stage
        Seq((source, Stage.SingleOpStage[A, A](identity)))
        
      case Plan.MapOp(sourcePlan, f) =>
        val stages = buildStagesRecursive(sourcePlan)
        extendLastStage(stages, Stage.map(f))
        
      case Plan.FilterOp(sourcePlan, p) =>
        val stages = buildStagesRecursive(sourcePlan)
        extendLastStage(stages, Stage.filter(p))
        
      case Plan.FlatMapOp(sourcePlan, f) =>
        val stages = buildStagesRecursive(sourcePlan)
        extendLastStage(stages, Stage.flatMap(f))
        
      case Plan.DistinctOp(sourcePlan) =>
        val stages = buildStagesRecursive(sourcePlan)
        extendLastStage(stages, Stage.distinct)
        
      case Plan.KeysOp(sourcePlan) =>
        val stages = buildStagesRecursive(sourcePlan)
        extendLastStage(stages, Stage.keys)
        
      case Plan.ValuesOp(sourcePlan) =>
        val stages = buildStagesRecursive(sourcePlan)
        extendLastStage(stages, Stage.values)
        
      case Plan.MapValuesOp(sourcePlan, f) =>
        val stages = buildStagesRecursive(sourcePlan)
        extendLastStage(stages.asInstanceOf[Seq[(Plan.Source[_], Stage[_, Any])]], Stage.mapValues(f).asInstanceOf[Stage[Any, A]])
        
      case Plan.FilterKeysOp(sourcePlan, p) =>
        val stages = buildStagesRecursive(sourcePlan)
        extendLastStage(stages.asInstanceOf[Seq[(Plan.Source[_], Stage[_, Any])]], Stage.filterKeys(p).asInstanceOf[Stage[Any, A]])
        
      case Plan.FilterValuesOp(sourcePlan, p) =>
        val stages = buildStagesRecursive(sourcePlan)
        extendLastStage(stages.asInstanceOf[Seq[(Plan.Source[_], Stage[_, Any])]], Stage.filterValues(p).asInstanceOf[Stage[Any, A]])
        
      case Plan.FlatMapValuesOp(sourcePlan, f) =>
        val stages = buildStagesRecursive(sourcePlan)
        extendLastStage(stages.asInstanceOf[Seq[(Plan.Source[_], Stage[_, Any])]], Stage.flatMapValues(f).asInstanceOf[Stage[Any, A]])
        
      case Plan.UnionOp(left, right) =>
        // Union creates a new stage boundary
        buildStagesRecursive(left) ++ buildStagesRecursive(right)
        
      case _ =>
        throw new UnsupportedOperationException(s"Stage building for $plan not implemented yet")
    }
  }
  
  
  /**
   * Extends the last stage in a sequence of stages with a new stage.
   * 
   * @param stages The sequence of stages to extend.
   * @param newStage The new stage to add to the sequence.
   * @return A new sequence of stages with the new stage added to the end.
   */
  private def extendLastStage[A, B, C](
    stages: Seq[(Plan.Source[_], Stage[_, B])], 
    newStage: Stage[B, C]
  ): Seq[(Plan.Source[_], Stage[_, C])] = {
    val initStages = stages.dropRight(1).asInstanceOf[Seq[(Plan.Source[_], Stage[_, C])]]
    val (source, lastStage) = stages.lastOption.getOrElse(throw new IllegalStateException("No stages to extend"))
    
    // Type safety note: This cast is safe and we can ignore the Any warnings because:
    // 1. stages.init contains stages that are not modified
    // 2. We're only changing the final output type expectation
    // 3. The actual stage execution preserves the transformation chain
    initStages :+ (source, Stage.ChainedStage(lastStage.asInstanceOf[Stage[Any, B]], newStage).asInstanceOf[Stage[Any, C]])
  }