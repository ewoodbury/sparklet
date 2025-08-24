package com.ewoodbury.sparklet.runtime

import scala.concurrent.duration.*

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.sparklet.core.{Partition, RetryPolicy}
import com.ewoodbury.sparklet.runtime.api.{RunnableTask, TaskScheduler}

class TestTaskSchedulerInterface extends AnyFlatSpec with Matchers {

  // Mock implementation for testing interface contract
  class MockTaskScheduler extends TaskScheduler[IO] {
    override def submit[A, B](tasks: Seq[RunnableTask[A, B]]): IO[Seq[Partition[B]]] =
      IO.pure(Seq.empty[Partition[B]])

    override def submitWithRetry[A, B](
      tasks: Seq[RunnableTask[A, B]],
      retryPolicy: RetryPolicy
    ): IO[Seq[Partition[B]]] =
      submit(tasks) // Simple implementation for testing

    override def shutdown(): IO[Unit] = IO.unit
  }

  "TaskScheduler interface" should "support both submit methods" in {
    val scheduler = new MockTaskScheduler()

    // Test that both methods can be called (interface contract)
    val tasks = Seq.empty[RunnableTask[Int, String]]
    val retryPolicy = RetryPolicy.default

    // These should compile and not throw exceptions
    val result1 = scheduler.submit(tasks)
    val result2 = scheduler.submitWithRetry(tasks, retryPolicy)
    val shutdownResult = scheduler.shutdown()

    // Verify they return the expected types
    result1 shouldBe a[IO[_]]
    result2 shouldBe a[IO[_]]
    shutdownResult shouldBe a[IO[_]]
  }

  it should "accept various retry policies" in {
    val scheduler = new MockTaskScheduler()
    val tasks = Seq.empty[RunnableTask[Int, String]]

    val policies = List(
      RetryPolicy.NoRetry,
      RetryPolicy.default,
      RetryPolicy.ExponentialBackoff(5, 2.seconds, 60.seconds),
      RetryPolicy.forExceptions(3, 1.second, 30.seconds)(classOf[RuntimeException])
    )

    // All should compile and work
    policies.foreach { policy =>
      val result = scheduler.submitWithRetry(tasks, policy)
      result shouldBe a[IO[_]]
    }
  }

  it should "maintain type safety across different task types" in {
    val scheduler = new MockTaskScheduler()

    // Test with different type parameters
    val intTasks = Seq.empty[RunnableTask[Int, Int]]
    val stringTasks = Seq.empty[RunnableTask[String, String]]
    val kvTasks = Seq.empty[RunnableTask[(String, Int), (String, Int)]]

    val policy = RetryPolicy.default

    // All should compile with correct types
    val intResult: IO[Seq[Partition[Int]]] = scheduler.submit(intTasks)
    val stringResult: IO[Seq[Partition[String]]] = scheduler.submit(stringTasks)
    val kvResult: IO[Seq[Partition[(String, Int)]]] = scheduler.submit(kvTasks)

    val intRetryResult: IO[Seq[Partition[Int]]] = scheduler.submitWithRetry(intTasks, policy)
    val stringRetryResult: IO[Seq[Partition[String]]] = scheduler.submitWithRetry(stringTasks, policy)
    val kvRetryResult: IO[Seq[Partition[(String, Int)]]] = scheduler.submitWithRetry(kvTasks, policy)

    // Verify types are preserved
    intResult shouldBe a[IO[_]]
    stringResult shouldBe a[IO[_]]
    kvResult shouldBe a[IO[_]]
    intRetryResult shouldBe a[IO[_]]
    stringRetryResult shouldBe a[IO[_]]
    kvRetryResult shouldBe a[IO[_]]
  }
}
