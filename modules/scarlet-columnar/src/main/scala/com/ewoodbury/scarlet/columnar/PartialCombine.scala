package com.ewoodbury.scarlet.columnar

/**
 * Partial-aggregate each morsel, then merge the partials of one input batch.
 *
 * The merge is the map-side combine: one partial per input batch goes to the exchange, instead of
 * one partial per morsel. A batch that is already a single morsel keeps that partial. Input
 * batches stay separate, so the exchange still sees one partial from each of them.
 */
object PartialCombine:

  def perBatch(
      batches: Vector[ColumnBatch],
      batchSize: Int,
      parallelism: Int,
      partial: ColumnBatch => ColumnBatch,
      merge: ColumnBatch => ColumnBatch,
  ): Vector[ColumnBatch] =
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (batches.isEmpty) then Vector.empty
    else
      val (sizes, partials) = partialMorsels(batches, batchSize, parallelism, partial)
      mergeMorsels(sizes, partials, parallelism, merge)

  /** One partial per morsel, in input order. `sizes` counts the morsels of each input batch. */
  def partialMorsels(
      batches: Vector[ColumnBatch],
      batchSize: Int,
      parallelism: Int,
      partial: ColumnBatch => ColumnBatch,
  ): (Vector[Int], Vector[ColumnBatch]) =
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    val groups = batches.map(batch => ColumnBatches.slice(batch, batchSize))
    val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
      partial(morsel)
    }
    (groups.map(_.length), partials)

  /** Merge the morsel partials of each input batch. A single morsel stays as it is. */
  def mergeMorsels(
      sizes: Vector[Int],
      partials: Vector[ColumnBatch],
      parallelism: Int,
      merge: ColumnBatch => ColumnBatch,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (sizes.isEmpty) then Vector.empty
    else
      val gathered = gather(sizes, partials)
      MorselScheduler.map(gathered.map(_.batch), parallelism) { (index, batch) =>
        val piece = gathered.lift(index).getOrElse {
          throw new IllegalArgumentException(s"missing partial $index")
        }
        if (piece.combine) then merge(batch) else batch
      }

  private final case class Piece(batch: ColumnBatch, combine: Boolean)

  private def gather(sizes: Vector[Int], partials: Vector[ColumnBatch]): Vector[Piece] =
    val seed: (Vector[ColumnBatch], Vector[Piece]) = (partials, Vector.empty[Piece])
    val (left, pieces) = sizes.foldLeft(seed) { (state, size) =>
      val (rest, acc) = state
      val (group, tail) = rest.splitAt(size)
      val piece =
        if (size <= 1) then
          Piece(
            group.headOption.getOrElse(
              throw new IllegalArgumentException("morsel group is empty"),
            ),
            combine = false,
          )
        else Piece(ColumnBatches.concat(group), combine = true)
      (tail, acc :+ piece)
    }
    if (left.nonEmpty) then
      throw new IllegalArgumentException(s"${left.length} partials were not grouped")
    pieces
