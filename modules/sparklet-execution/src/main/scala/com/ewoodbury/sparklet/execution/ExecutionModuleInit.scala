package com.ewoodbury.sparklet.execution

/**
 * Initialization entry point for the execution module. This ensures that the
 * DefaultExecutionService is registered when the execution module is loaded.
 */
object ExecutionModuleInit {
  // Force initialization of the DefaultExecutionService
  private val registration = DefaultExecutionService.initialize()

  /**
   * Call this method to ensure the execution module is properly initialized. This is idempotent
   * and safe to call multiple times.
   */
  def initialize(): Unit = {
    // Nothing to do - initialization happens in the private val above
    registration
  }
}
