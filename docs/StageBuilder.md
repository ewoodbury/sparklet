# StageBuilder

`StageBuilder` compiles logical plans into executable stage graphs. It is the boundary between
the logical `Plan` vocabulary (sparklet-core) and the physical `Stage`/`Operation`/`WideOp`
vocabulary (sparklet-execution).

## Output model

```scala
case class StageGraph(
  stages: Map[StageId, StageInfo],
  dependencies: Map[StageId, Set[StageId]], // child -> parents
  finalStageId: StageId,
)

case class StageInfo(
  id: StageId,
  stage: Stage[_, _],                 // fused narrow closures, or identity for shuffle stages
  inputSources: Seq[InputSource],
  isShuffleStage: Boolean,
  wideOp: Option[WideOp],             // set exactly when isShuffleStage — the dispatch record
  outputPartitioning: Option[PartitioningInfo],
)
```

Input sources:

- `SourceInput(partitions)` — original data partitions.
- `StageOutput(stageId)` — in-memory output of a computed upstream stage (no shuffle).
- `ShuffleInput(stageId, side, numPartitions)` — shuffle service read; `side` (`Left`/`Right`)
  disambiguates multi-input operations (join/cogroup).

`PartitioningInfo` is distribution, ordering, and layout. The row path always uses
`Layout.BoxedRows`.

- `Hash(n, Seq(0))` — key hash (`groupByKey`, `reduceByKey`, `partitionBy`, join, cogroup), or a
  bypass chained after one. `Seq(0)` is the pair key, not a schema column.
- `Range(n, Seq(0))` plus `Sorted` — `sortBy`. Ascending is empty because a row-path `Ordering`
  does not expose direction.
- `RoundRobin(n)` — `repartition`.
- `Unknown(n)` — a source, or `coalesce`. The width is known; co-location is not.
- `Singleton` — one partition holding the whole result. The row path does not emit it yet.

Shuffle bypass reads distribution, not layout. Keyed ops bypass only on `Hash` at the target
width. `repartition` bypasses on any non-hash distribution with the same width. This is not
inferred from record order.

## Construction algorithm

`buildStageGraph` walks the plan recursively, accumulating operations into stage drafts:

- **Narrow operations** (`map`, `filter`, `flatMap`, `distinct`, KV projections,
  `mapPartitions`): appended onto the current stage when it is not a shuffle stage. Any
  non-shuffle stage is chainable, including union concatenation stages — narrow ops run per
  partition of the stage's output. This is what fuses `map(f).filter(p)` into one pass and lets
  narrow work follow a shuffle without a data movement boundary.
- **Wide operations** (`groupByKey`, `reduceByKey`, `sortBy`, `partitionBy`, `repartition`,
  `coalesce`, `join`, `cogroup`): cut a stage boundary via `createShuffleStageUnified`, which
  attaches the `WideOp` dispatch record, `ShuffleInput` sources (side-tagged for
  join/cogroup), and output partitioning metadata.
- **Shuffle bypass** (`Operation.canBypassShuffle`): when upstream partitioning already matches
  the operation's requirement, the operation is appended as a local op instead of a shuffle
  stage — e.g. `partitionBy(n)` then `groupByKey` with matching `n` becomes a local group.
  Coalesce never bypasses.
- **Union**: creates a concatenation stage with `StageOutput` inputs from both branches.

## Materialization

Accumulated `Vector[Operation[Any, Any]]` materializes into executable `Stage` closures:
single operations become `SingleOpStage` directly, chains fold into `ChainedStage`. The
Plan-to-Operation conversion happens in one named erasure boundary, `Operation.fromPlan`
(see ARCHITECTURE.md for the erasure policy).

## Validation

Every produced graph is validated before returning (`validateStageGraph`, also exercised
directly by `TestStageGraphValidation`):

- `finalStageId` exists; every dependency references existing stages.
- No self-dependencies; the graph is acyclic (DFS).
- Every stage is reachable from `finalStageId` (orphans rejected).
- Stage IDs start at 0 (gaps only warn, to future-proof ID reuse).
- Shuffle stages carry a `WideOp`; multi-input shuffles have exactly one `Left` and one `Right`
  input with matching partition counts.
- Partitioning metadata is sane (hash, range, and round-robin widths are positive; counts are
  bounded; hash and range name at least one key ordinal).

Validation failures are `IllegalStateException` with the stage IDs involved.
