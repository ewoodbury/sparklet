package com.ewoodbury.sparklet.localengine

object LocalExecutor:

  /**
   * Executes a Plan locally, interpreting the operations.
   * Note: This implementation is simple and not optimized (e.g., for fusion).
   * It demonstrates the principle of interpreting the plan.
   *
   * Type safety relies on the correct construction of Plan via DistCollection methods.
   */
  def execute[A](plan: Plan[A]): Iterable[A] = {
    println(s"Executing plan node: $plan") // Log execution step
    plan match {
      case Plan.Source(dataSource) =>
        println(" -> Executing Source")
        val data = dataSource()
        println(s" -> Source data materialized (first few): ${data.take(5).mkString("[", ", ", "...]")}")
        data // Return the source data

      case Plan.MapOp(source, mapFunction) =>
        println(s" -> Executing MapOp")
        // Recursively execute the source plan
        val sourceResults = execute(source)
        // Apply the function f (which is I => A)
        val results = sourceResults.map(mapFunction)
        println(s" -> MapOp applied (first few results): ${results.take(5).mkString("[", ", ", "...]")}")
        results

      case Plan.FilterOp(source, predicateFunction) =>
        println(s" -> Executing FilterOp")
        // Recursively execute the source plan
        val sourceResults = execute(source) // Results have type A
        // Apply the predicate p (which is A => Boolean)
        val results = sourceResults.filter(predicateFunction)
        println(s" -> FilterOp applied (first few results): ${results.take(5).mkString("[", ", ", "...]")}")
        results

      case Plan.FlatMapOp(source, flatMapFunction) =>
        println(s" -> Executing FlatMapOp")
        // Recursively execute the source plan
        val sourceResults = execute(source)
        // Apply the function f (which is I -> IterableOnce[A])
        val results = sourceResults.flatMap(flatMapFunction)
        println(s" -> FlatMapOp applied (first few results): ${results.take(5).mkString("[", ", ", "...]")}")
        results

      case Plan.DistinctOp(source) =>
        println(s" -> Executing DistinctOp")
        val sourceResults = execute(source)
        val results = sourceResults.toSeq.distinct
        println(s" -> DistinctOp applied (first few results): ${results.take(5).mkString("[", ", ", "...]")}")
        results

      case Plan.UnionOp(left, right) =>
        println(s" -> Executing UnionOp")
        val leftResults = execute(left)
        val rightResults = execute(right)
        val results = leftResults ++ rightResults
        println(s" -> UnionOp applied (first few results): ${results.take(5).mkString("[", ", ", "...]")}")
        results
    }
  }
end LocalExecutor