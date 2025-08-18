package com.ewoodbury.sparklet.core

/**
 * Service Provider Interface for executing plans. This allows the API module to trigger execution
 * without depending on the execution module.
 */
trait ExecutionService {

  /**
   * Executes a plan and returns the results.
   */
  def execute[A](plan: Plan[A]): Seq[A]

  /**
   * Executes a plan and returns the count of results.
   */
  def count[A](plan: Plan[A]): Long

  /**
   * Executes a plan and returns the first n results.
   */
  def take[A](plan: Plan[A], n: Int): Seq[A]
}

/**
 * Global registry for the execution service. The execution module will register an implementation
 * here.
 */
object ExecutionService {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var current: ExecutionService = new ExecutionService {
    def execute[A](plan: Plan[A]): Seq[A] = {
      throw new UnsupportedOperationException(
        "No execution service registered. Please ensure the execution module is properly initialized.",
      )
    }

    def count[A](plan: Plan[A]): Long = {
      throw new UnsupportedOperationException(
        "No execution service registered. Please ensure the execution module is properly initialized.",
      )
    }

    def take[A](plan: Plan[A], n: Int): Seq[A] = {
      throw new UnsupportedOperationException(
        "No execution service registered. Please ensure the execution module is properly initialized.",
      )
    }
  }

  def get: ExecutionService = current

  def set(service: ExecutionService): Unit = {
    current = service
  }
}
