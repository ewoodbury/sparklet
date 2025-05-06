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

      case Plan.MapOp(source, f) =>
        println(s" -> Executing MapOp")
        // Recursively execute the source plan
        val sourceResults = execute(source) // Results have type I for some I
        // Apply the function f (which is I => A)
        // We need to cast `f` because the type `I` is lost in the recursive call's return type.
        val typedF = f.asInstanceOf[Any => A]
        val results = sourceResults.map(typedF)
        println(s" -> MapOp applied (first few results): ${results.take(5).mkString("[", ", ", "...]")}")
        results

      case Plan.FilterOp(source, p) =>
        println(s" -> Executing FilterOp")
        // Recursively execute the source plan
        val sourceResults = execute(source) // Results have type A
        // Apply the predicate p (which is A => Boolean)
        val results = sourceResults.filter(p)
        println(s" -> FilterOp applied (first few results): ${results.take(5).mkString("[", ", ", "...]")}")
        results
    }
  }
end LocalExecutor