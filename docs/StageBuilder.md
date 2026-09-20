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
  outputPartitioning: Option[Partitioning],
)
```

Input sources:

- `SourceInput(partitions)` — original data partitions.
- `StageOutput(stageId)` — in-memory output of a computed upstream stage (no shuffle).
- `ShuffleInput(stageId, side, numPartitions)` — shuffle service read; `side` (`Left`/`Right`)
  disambiguates multi-input operations (join/cogroup).

`Partitioning(byKey, numPartitions)`: `byKey = true` means the data was written through the key
hash partitioner (true shuffle stages, or bypass-chained local ops after one) — never inferred
from stable record order. The shuffle-bypass optimization trusts this contract.

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
- Partitioning metadata is sane (`byKey` implies positive count; counts bounded).

Validation failures are `IllegalStateException` with the stage IDs involved.
