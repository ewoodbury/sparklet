# Architecture

Sparklet is a data processing engine inspired by Spark, written in pure functional Scala 3. This
document describes the system as it exists today: one execution path, a clean logical/physical
split, and type erasure confined to named boundaries.

## Module layout

| Module | Responsibility |
|---|---|
| `sparklet-api` | `DistCollection`: the user-facing lazy API. Builds `Plan` trees, triggers actions. |
| `sparklet-core` | `Plan` ADT, `PlanWide`, `Partition`, `SparkletConf`, `ExecutionService` SPI, IDs. |
| `sparklet-execution` | The compiler and runtime: `StageBuilder`, `Stage`, `Operation`/`WideOp`, `DAGScheduler`, `ExecutionPlanner`, `StageExecutor`, `ShuffleHandler`, `JoinExecutor`, `Task`. |
| `sparklet-runtime` | Execution SPIs (`TaskScheduler`, `ShuffleService`, `Partitioner`, `BroadcastService`) and local in-memory implementations. |
| `sparklet-columnar` | Primitive column batches and encoders. Execution does not call it yet. |
| `sparklet-tests` | Aggregated ScalaTest suite (sequential by design; see Testing notes). |

Dependency direction: `api -> core`; `execution -> api, core, runtime`; `runtime -> core`;
`columnar` depends on nothing. The logical layer cannot name physical types — the split is
enforced by the build.

## Logical vs physical

Two vocabularies, separated at the module level:

- **Logical** (`sparklet-core`): `Plan[A]` — an immutable, lazy tree of what the user asked for.
  `DistCollection` transformations only prepend nodes; nothing runs until an action.
- **Physical** (`sparklet-execution`): `StageGraph` — stages, dependencies, shuffle boundaries.
  Narrow operations are fused into stages; wide operations become shuffle stages carrying a
  structured `WideOp` record.

The rule: `Plan` is legal in the compiler (`StageBuilder` reads it to compile), illegal in the
executor (`StageExecutor`/`ShuffleHandler`/`ExecutionPlanner` dispatch only on `WideOp`).

## The execution path (the only one)

```text
DistCollection action
  -> ExecutionService (SPI, registered by the execution module)
  -> DefaultExecutionService
  -> DAGScheduler.executePartitions
  -> StageBuilder.buildStageGraph        (plan -> stage graph)
  -> TopologicalSort                     (execution order)
  -> ExecutionPlanner.runStages          (per stage:)
       StageExecutor.getInputPartitions  (SourceInput | StageOutput | ShuffleInput)
       StageExecutor.executeStage        (WideOp dispatch; one task per partition via TaskScheduler)
       ExecutionPlanner.writeShuffleIfNeeded (ShuffleWriteReason decides the write shape)
  -> final stage partitions, flattened only at the action boundary
```

A bare `Plan.Source` short-circuits to its partitions (no tasks needed for identity work).
Narrow-only plans compile to a one-stage graph and flow through the same path as wide plans.

## Compilation: stages and fusion

`StageBuilder` walks the plan recursively:

- Narrow operations chain onto the current stage (`appendOperation`); their operations accumulate
  into a `Vector[Operation]` materialized as fused `Stage` closures — one pass per partition.
- Wide operations cut a stage boundary (`createShuffleStageUnified`), producing a `WideOp`
  (`GroupByKeyWideOp`, `SortByWideOp`, `JoinWideOp`, ...) plus `ShuffleInput` sources and
  `outputPartitioning` metadata.
- Shuffle bypass: when upstream partitioning metadata already matches the wide operation's
  requirement (`Operation.canBypassShuffle`), the operation is appended as a local no-op instead
  of creating a shuffle stage. Example: `partitionBy(4)` then `groupByKey` (default 4) is a local
  group. Bypass reads distribution, not layout: keyed ops require `Distribution.Hash` at the
  target width; `repartition` skips a shuffle for any other distribution with that width.
- `PartitioningInfo` is distribution, ordering, and layout. This path emits `Layout.BoxedRows`.
  Sources are `Unknown(width)`. Keyed shuffles are `Hash`. `sortBy` is `Range` plus `Sorted`;
  `map`, `flatMap`, and `mapPartitions` drop both. A repartition or coalesce shuffle is also
  `Unknown`: the writer hashes the element, which is not pair-key hash and not round-robin. A
  bypassed repartition does not move rows, so it keeps the upstream description. Hash and range
  keys are `Seq(0)`, the logical key, not a schema column. `RoundRobin` and `Singleton` are not
  emitted yet.
- Narrow work after a shuffle reads the upstream stage's output in memory (`StageOutput`) — no
  extra shuffle or materialization round-trip.
- The graph is validated (existence, acyclicity, reachability, partitioning sanity,
  side-tag consistency) before execution; see `TestStageGraphValidation` for the enforced
  invariants.

## Shuffle write policy

`ExecutionPlanner` writes a stage's output only when a dependent stage is a shuffle stage, and
the write shape is a value: `ShuffleWriteReason.forDependents` picks, by priority — sortBy
(range partitioned to preserve global order), then partitionBy (key-hashed into its target
count), then repartition/coalesce, then a combine-then-hash write when every dependent is the
same `reduceByKey`, then a generic keyed write. Shuffle IDs come from the `ShuffleService`,
never from stage IDs. `reduceByKey`'s function is treated as associative and commutative.

## Type erasure policy

`Plan[A]` is invariant and stages transport `Partition[_]`, so some casts are unavoidable.
Policy (compiler-enforced — `Wart.AsInstanceOf` and `Wart.Any` are errors):

- Casts live only at named erasure boundaries, each with a suppression and a comment explaining
  why the cast is safe: `Operation.fromPlan`, `DistCollection.kvPlan`,
  `StageBuilder`/`StageExecutor`/`JoinExecutor`/`ShuffleHandler` (file-level, documented),
  `Task.BroadcastHashJoinTask`, `DAGScheduler.executePartitions`, and the storage
  implementations (`LocalShuffleService`, `LocalBroadcastService`).
- No new cast sites outside these boundaries without a suppression and a justification.

## Runtime SPIs

| SPI | Local impl | Role |
|---|---|---|
| `TaskScheduler[F]` | `LocalTaskScheduler` (thread pool, bounded parallelism) | Runs tasks; applies the `SparkletConf`-derived retry policy at submission time |
| `ShuffleService` | `LocalShuffleService` (in-memory, concurrent map) | Shuffle write/read keyed by `ShuffleId` + `PartitionId` |
| `Partitioner` | `HashPartitioner` (`floorMod` — safe for negative hashes) | Key -> partition assignment |
| `BroadcastService` | `LocalBroadcastService` | Broadcast join support |

`SparkletRuntime` wires these into `RuntimeComponents` (a global holder with a thread-local
override used by tests).

## Failure handling

Tasks retry per `SparkletConf` (`maxTaskRetries`, exponential backoff) inside
`TaskExecutionWrapper`; permanent failure propagates to the caller. Lineage-based recovery was
removed: it could not safely reconstruct arbitrary user functions. Retry is honest and tested
(`TestLocalTaskSchedulerRetry`).

## Ordering guarantees

- `collect`/`take`/`count` preserve partition order and within-partition order.
- `sortBy` produces a globally ordered result: sampling-based range partitioning, a local sort
  per range partition, and concatenation in partition order. There is no driver-side merge.
- `union` concatenates left then right.
- `groupBy`-family outputs group by key equality; record order inside groups follows input order.
- `groupByKey`/`reduceByKey`/`cogroup`/`sortBy` keep one output partition per shuffle partition
  and run as one task per partition, matching joins, `repartition`, `coalesce`, and `partitionBy`.
  A given key of a keyed aggregation lives in exactly one output partition.

## Testing notes

- Tests run sequentially (`Test / parallelExecution := false`): `SparkletConf`,
  `SparkletRuntime`, and `ExecutionService` are process-global; suites mutate them. Parallelism
  returns when global state becomes injected (deferred).
- `SparkletConf` is global mutable state: tests that change it restore defaults in `afterEach`.
- Retry tests set `baseRetryDelayMs = 1L` or they take seconds.
- Test logs are silenced to WARN via `modules/sparklet-tests/src/test/resources/log4j2-test.xml`.

## Known limitations (deferred work)

Roadmap and sequencing are tracked externally; the headline items:

- No cross-branch dedup: reusing a `DistCollection` in two places recomputes it; at most one
  shuffle write per stage.
- No rule-based logical optimizer (predicate/projection pushdown) — the `PhysicalPlan` layer
  precedes this work.
- Sort-merge join is a hash grouping wearing a sort-merge name; a true typed sort-merge join is
  deferred.
