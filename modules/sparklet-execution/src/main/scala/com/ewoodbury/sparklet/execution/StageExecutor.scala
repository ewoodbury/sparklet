package com.ewoodbury.sparklet.execution

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.typesafe.scalalogging.StrictLogging

import com.ewoodbury.sparklet.core.{Partition, PartitionId, Plan, ShuffleId, StageId}
import com.ewoodbury.sparklet.runtime.api.{ShuffleService, TaskScheduler}

/**
 * Executor for handling stage execution.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.MutableDataStructures",
    "org.wartremover.warts.Equals",
  ),
)
final class StageExecutor[F[_]: Sync](
    shuffle: ShuffleService,
    scheduler: TaskScheduler[F],
    joinExecutor: JoinExecutor[F],
) extends StrictLogging:

  /**
   * Gets input partitions for a stage, returning them with a wildcard type `_`. This avoids
   * casting at the source, deferring it to the execution function.
   */
  def getInputPartitionsForStage(
      stageInfo: StageBuilder.StageInfo,
      stageResults: Map[StageId, Seq[Partition[_]]],
      stageToShuffleId: Map[StageId, ShuffleId],
  ): Seq[Partition[_]] = {
    stageInfo.inputSources.flatMap {
      case StageBuilder.SourceInput(partitions) =>
        partitions // No cast needed

      case StageBuilder.StageOutput(depStageId) =>
        // Read the already computed output of the dependent stage
        stageResults.getOrElse(depStageId, Seq.empty[Partition[_]])

      case StageBuilder.ShuffleFrom(upstreamStageId, numPartitions) =>
        val actualShuffleId = stageToShuffleId.getOrElse(
          upstreamStageId,
          throw new IllegalStateException(
            s"Missing shuffle id for upstream stage ${upstreamStageId.toInt} feeding stage ${stageInfo.id.toInt}",
          ),
        )
        (0 until numPartitions).map { partitionIdx =>
          shuffle.readPartition[Any, Any](actualShuffleId, PartitionId(partitionIdx))
        }

      case StageBuilder.TaggedShuffleFrom(upstreamStageId, _side, numPartitions) =>
        // Explicitly resolve upstream shuffle ID and read exactly those partitions.
        // Used for join and cogroup operations.
        val actualShuffleId = stageToShuffleId.getOrElse(
          upstreamStageId,
          throw new IllegalStateException(
            s"Missing shuffle id for upstream stage ${upstreamStageId.toInt} feeding stage ${stageInfo.id.toInt}",
          ),
        )
        (0 until numPartitions).map { partitionIdx =>
          shuffle.readPartition[Any, Any](actualShuffleId, PartitionId(partitionIdx))
        }
    }
  }

  /**
   * Executes a single stage, dispatching to a narrow or shuffle implementation. This function now
   * handles the necessary casting based on the stage type.
   */

  def executeStage(
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[_]],
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    if (inputPartitions.isEmpty) then
      Sync[F].delay(logger.warn(s"Stage ${stageInfo.id.toInt} has no input partitions")) *> Sync[F]
        .pure(Seq.empty)
    else {
      stageInfo match {
        case info if info.isShuffleStage =>
          val partitions = inputPartitions.asInstanceOf[Seq[Partition[(Any, Any)]]]
          executeShuffleStage(info, partitions, stageToShuffleId)
        case info =>
          val anyPartitions = inputPartitions.asInstanceOf[Seq[Partition[Any]]]
          executeNarrowStage(info, anyPartitions, stageToShuffleId)
      }
    }
  }

  /**
   * Executes a narrow transformation stage. Now generic for input (T) and output (U) types.
   */
  private def executeNarrowStage[A, B](
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[A]],
      @annotation.unused stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    val stage = stageInfo.stage.asInstanceOf[Stage[A, B]]
    val tasks = inputPartitions.map { partition => Task.StageTask(partition, stage) }
    scheduler.submit(tasks).map(_.asInstanceOf[Seq[Partition[_]]])
  }

  /**
   * Executes a shuffle stage by applying the appropriate shuffle operation. This version uses
   * generics to provide type safety for keys (K) and values (V).
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def executeShuffleStage[K, V](
      stageInfo: StageBuilder.StageInfo,
      inputPartitions: Seq[Partition[(K, V)]],
      stageToShuffleId: Map[StageId, ShuffleId],
  ): F[Seq[Partition[_]]] = {
    // Join is the only shuffle op that currently requires effectful scheduling work here
    stageInfo.shuffleOperation match {
      case Some(joinOp: Plan.JoinOp[_, _, _]) =>
        val tagged = stageInfo.inputSources.collect { case t: StageBuilder.TaggedShuffleFrom => t }
        val leftTagged = tagged
          .find(_.side == StageBuilder.Side.Left)
          .getOrElse(
            throw new IllegalStateException(
              s"Join missing Left input for stage ${stageInfo.id.toInt}",
            ),
          )
        val rightTagged = tagged
          .find(_.side == StageBuilder.Side.Right)
          .getOrElse(
            throw new IllegalStateException(
              s"Join missing Right input for stage ${stageInfo.id.toInt}",
            ),
          )
        val leftShuffleId = stageToShuffleId.getOrElse(
          leftTagged.stageId,
          throw new IllegalStateException(
            s"Missing shuffle id for Left upstream stage ${leftTagged.stageId.toInt}",
          ),
        )
        val rightShuffleId = stageToShuffleId.getOrElse(
          rightTagged.stageId,
          throw new IllegalStateException(
            s"Missing shuffle id for Right upstream stage ${rightTagged.stageId.toInt}",
          ),
        )
        val numPartitions = leftTagged.numPartitions // assume both sides same for now

        // Determine join strategy: use hint if available, otherwise auto-select
        val strategy =
          joinOp.joinStrategy.getOrElse(
            joinExecutor.selectJoinStrategy(leftShuffleId, rightShuffleId),
          )

        logger.info(s"Using join strategy: $strategy for stage ${stageInfo.id.toInt}")

        strategy match {
          case Plan.JoinStrategy.Broadcast =>
            joinExecutor.executeBroadcastHashJoin(leftShuffleId, rightShuffleId, numPartitions)
          case Plan.JoinStrategy.SortMerge =>
            joinExecutor.executeSortMergeJoin(leftShuffleId, rightShuffleId, numPartitions)
          case Plan.JoinStrategy.ShuffleHash =>
            joinExecutor.executeShuffleHashJoin(leftShuffleId, rightShuffleId, numPartitions)
        }
      case _ =>
        Sync[F].delay(
          logger.debug(
            s"Executing shuffle stage ${stageInfo.id.toInt} with ${inputPartitions.size} input partitions",
          ),
        ) *>
          Sync[F].delay {
            val resultPartitions: Seq[Partition[_]] = stageInfo.shuffleOperation match {

              // Pattern match to capture the element type `a` and sorting key type `s`.
              case Some(sortBy: Plan.SortByOp[a, s]) =>
                // Input for sort is now key-value pairs of (sortKey, element)
                val typedPartitions = inputPartitions.asInstanceOf[Seq[Partition[(s, a)]]]
                implicit val ord: Ordering[s] = sortBy.ordering

                // Sort within each partition by key
                val sortedIters: Seq[Iterator[(s, a)]] =
                  typedPartitions.map(p => p.data.toSeq.sortBy(_._1)(ord).iterator)

                // K-way merge across partitions to ensure global order
                import scala.collection.mutable
                case class Head(idx: Int, pair: (s, a))
                implicit val heapOrd: Ordering[Head] = Ordering.by(_.pair._1)
                val heap = mutable.PriorityQueue.empty[Head](heapOrd.reverse)
                sortedIters.zipWithIndex.foreach { case (it, i) =>
                  if (it.hasNext) heap.enqueue(Head(i, it.next()))
                }
                val buffers = sortedIters.toArray

                val merged = mutable.ArrayBuffer[a]()
                while (heap.nonEmpty) {
                  val h = heap.dequeue()
                  merged += h.pair._2
                  val i = h.idx
                  if (buffers(i).hasNext) heap.enqueue(Head(i, buffers(i).next()))
                }

                Seq(Partition(merged.toSeq))

              // The remaining shuffle operations work on standard (K, V) pairs.
              case Some(Plan.GroupByKeyOp(_)) =>
                val allData = inputPartitions.flatMap(_.data)
                val groupedData = allData.groupBy(_._1).map { case (key, pairs) =>
                  (key, pairs.map(_._2))
                }
                Seq(Partition(groupedData.toSeq)).asInstanceOf[Seq[Partition[Any]]]

              case Some(reduceByKey: Plan.ReduceByKeyOp[_, _]) =>
                val allData = inputPartitions.flatMap(_.data)
                val reduceFunc = reduceByKey.reduceFunc.asInstanceOf[(V, V) => V]
                val reducedData = allData.groupBy(_._1).map { case (key, pairs) =>
                  val reducedValue = pairs
                    .map(_._2)
                    .reduceOption(reduceFunc)
                    .getOrElse(throw new NoSuchElementException(s"No values found for key $key"))
                  (key, reducedValue)
                }
                Seq(Partition(reducedData.toSeq))

              case Some(_: Plan.CoGroupOp[_, _, _]) =>
                val tagged = stageInfo.inputSources.collect {
                  case t: StageBuilder.TaggedShuffleFrom =>
                    t
                }
                val leftTagged = tagged
                  .find(_.side == StageBuilder.Side.Left)
                  .getOrElse(
                    throw new IllegalStateException(
                      s"Cogroup missing Left input for stage ${stageInfo.id.toInt}",
                    ),
                  )
                val rightTagged = tagged
                  .find(_.side == StageBuilder.Side.Right)
                  .getOrElse(
                    throw new IllegalStateException(
                      s"Cogroup missing Right input for stage ${stageInfo.id.toInt}",
                    ),
                  )
                val leftShuffleId = stageToShuffleId.getOrElse(
                  leftTagged.stageId,
                  throw new IllegalStateException(
                    s"Missing shuffle id for Left upstream stage ${leftTagged.stageId.toInt}",
                  ),
                )
                val rightShuffleId = stageToShuffleId.getOrElse(
                  rightTagged.stageId,
                  throw new IllegalStateException(
                    s"Missing shuffle id for Right upstream stage ${rightTagged.stageId.toInt}",
                  ),
                )
                val numPartitions = leftTagged.numPartitions
                val leftData = (0 until numPartitions).flatMap { partitionId =>
                  shuffle.readPartition[Any, Any](leftShuffleId, PartitionId(partitionId)).data
                }
                val rightData = (0 until numPartitions).flatMap { partitionId =>
                  shuffle.readPartition[Any, Any](rightShuffleId, PartitionId(partitionId)).data
                }

                // Group left and right data by key
                val leftByKey = leftData.groupBy(_._1)
                val rightByKey = rightData.groupBy(_._1)

                // Perform cogroup - include all keys from both sides
                val allKeys = leftByKey.keySet ++ rightByKey.keySet
                val cogroupedData = allKeys.map { key =>
                  val leftValues = leftByKey.getOrElse(key, Seq.empty).map(_._2)
                  val rightValues = rightByKey.getOrElse(key, Seq.empty).map(_._2)
                  (key, (leftValues, rightValues))
                }
                Seq(Partition(cogroupedData.toSeq))

              case Some(_: Plan.RepartitionOp[_]) | Some(_: Plan.CoalesceOp[_]) |
                  Some(_: Plan.PartitionByOp[_, _]) =>
                val typed = inputPartitions.asInstanceOf[Seq[Partition[(Any, Unit)]]]
                val out = typed.map { p => Partition(p.data.iterator.map(_._1).toList) }
                out

              // A default GroupByKey for any other unhandled shuffle operation.
              case _ =>
                val allData = inputPartitions.flatMap(_.data)
                val groupedData = allData.groupBy(_._1).map { case (key, pairs) =>
                  (key, pairs.map(_._2))
                }
                Seq(Partition(groupedData.toSeq))
            }
            resultPartitions
          }
    }
  }
