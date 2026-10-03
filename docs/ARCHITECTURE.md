# Architecture

Scarlet is a data processing engine inspired by Spark, written in pure functional Scala 3. This
document describes the system as it exists today: a row path and a columnar path for `Int`,
`(Int, Int)`, and `String` filter and map, a clean logical/physical split, and type erasure
confined to named boundaries.

## Module layout

| Module | Responsibility |
|---|---|
| `scarlet-api` | `DistCollection`: the user-facing lazy API. Builds `Plan` trees, triggers actions. |
| `scarlet-core` | `Plan` ADT, `PlanWide`, `Partition`, `ScarletConf`, `ExecutionService` SPI, IDs. |
| `scarlet-execution` | The compiler and runtime: `StageBuilder`, `Stage`, `Operation`/`WideOp`, `DAGScheduler`, `ExecutionPlanner`, `StageExecutor`, `ShuffleHandler`, `JoinExecutor`, `Task`. |
| `scarlet-runtime` | Execution SPIs (`TaskScheduler`, `ShuffleService`, `Partitioner`, `BroadcastService`) and local in-memory implementations. |
| `scarlet-columnar` | Column batches, vector kernels, hash aggregate and join, and a local hash exchange. |
| `scarlet-tests` | Aggregated ScalaTest suite (sequential by design; see Testing notes). |

Dependency direction: `api -> core`; `execution -> api, core, runtime, columnar`; `runtime -> core`;
`columnar` depends on nothing. The logical layer cannot name physical types — the split is
enforced by the build.

## Logical vs physical

Two vocabularies, separated at the module level:

- **Logical** (`scarlet-core`): `Plan[A]` — an immutable, lazy tree of what the user asked for.
  `DistCollection` transformations only prepend nodes; nothing runs until an action.
- **Physical** (`scarlet-execution`): `StageGraph` — stages, dependencies, shuffle boundaries.
  Narrow operations are fused into stages; wide operations become shuffle stages carrying a
  structured `WideOp` record. The columnar path has a second tree, `PhysicalOp`, with `Scan`,
  `Filter`, `Project`, `Pipeline`, `Exchange`, `HashAggregate`, and `HashJoin` nodes.
  `ColumnarPlanner.lower` builds it and does not run user functions. Straight int, double, and
  string filter/project runs of two to four steps use one fused pipeline kernel; longer runs are
  cut into chunks of four. Pair operations, aggregates, and joins remain separate nodes.
  `Exchange` is the boundary under an aggregate and on each side of a join; those kernels perform
  the shuffle.

The rule: `Plan` is legal in the compiler (`StageBuilder` reads it to compile), illegal in the
executor (`StageExecutor`/`ShuffleHandler`/`ExecutionPlanner` dispatch only on `WideOp`).

## The execution path

```text
DistCollection action
  -> ExecutionService (SPI, registered by the execution module)
  -> DefaultExecutionService
       bare Plan.Source: return its partitions
       ColumnarPlanner.tryRun: lower an encodable Int, Double, (Int, Int), or String plan to PhysicalOp, then interpret
       otherwise DAGScheduler.executePartitions
            -> StageBuilder.buildStageGraph
            -> TopologicalSort
            -> ExecutionPlanner.runStages
                 StageExecutor.getInputPartitions
                 StageExecutor.executeStage
                 ExecutionPlanner.writeShuffleIfNeeded
  -> final partitions, flattened only at the action boundary
```

A bare `Plan.Source` short-circuits to its partitions. `ColumnarPlanner` runs filter, map,
filterKeys, filterValues, reduceByKey, and inner join when the values it sees are `Int` or
`(Int, Int)`, and filter and map when they are `Double` or `String`, if
`ScarletConf.columnarExecution` is true (the default). Any other node keeps the whole plan on
the row path. `Double` is the float64 column. A 32-bit `Float` stays on the row path. Strings are a
dictionary of JVM strings plus int codes (`Utf8Dict`). A null string is a value, so filter and
map see it. Group-by and join stay on int keys. Narrow columnar ops keep input order and the
input partition count. `reduceByKey` and `join` hash with `floorMod` into
`defaultShufflePartitions`.
`columnarBatchSize` (default 8192) is the morsel width. Set `columnarExecution` false to force
the row path.

On the row path, narrow-only plans compile to a one-stage graph and flow through the same path
as wide plans.

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
- `PartitioningInfo` is distribution, ordering, and layout. The row path emits `Layout.BoxedRows`.
  `ColumnarPlanner.partitioning` reports `Layout.Columnar` for a plan it can run, and `None`
  otherwise.
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
  `Task.BroadcastHashJoinTask`, `DAGScheduler.executePartitions`, `ColumnarPlanner`, and the
  storage implementations (`LocalShuffleService`, `LocalBroadcastService`).
- No new cast sites outside these boundaries without a suppression and a justification.

## Runtime SPIs

| SPI | Local impl | Role |
|---|---|---|
| `TaskScheduler[F]` | `LocalTaskScheduler` (thread pool, bounded parallelism) | Runs tasks; applies the `ScarletConf`-derived retry policy at submission time |
| `ShuffleService` | `LocalShuffleService` (in-memory, concurrent map) | Shuffle write/read keyed by `ShuffleId` + `PartitionId` |
| `Partitioner` | `HashPartitioner` (`floorMod` — safe for negative hashes) | Key -> partition assignment |
| `BroadcastService` | `LocalBroadcastService` | Broadcast join support |

`ScarletRuntime` wires these into `RuntimeComponents` (a global holder with a thread-local
override used by tests).

## Failure handling

Tasks retry per `ScarletConf` (`maxTaskRetries`, exponential backoff) inside
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

- Tests run sequentially (`Test / parallelExecution := false`): `ScarletConf`,
  `ScarletRuntime`, and `ExecutionService` are process-global; suites mutate them. Parallelism
  returns when global state becomes injected (deferred).
- `ScarletConf` is global mutable state: tests that change it restore defaults in `afterEach`.
- Retry tests set `baseRetryDelayMs = 1L` or they take seconds.
- Test logs are silenced to WARN via `modules/scarlet-tests/src/test/resources/log4j2-test.xml`.

## Known limitations (deferred work)

Roadmap and sequencing are tracked externally; the headline items:

- No cross-branch dedup: reusing a `DistCollection` in two places recomputes it; at most one
  shuffle write per stage.
- Fusion is limited to straight int, double, and string filter/project runs of up to four steps.
  There is no cost model and no predicate pushdown.
- Sort-merge join is a hash grouping wearing a sort-merge name; a true typed sort-merge join is
  deferred.
