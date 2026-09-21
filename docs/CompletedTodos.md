# Completed Todos

Historical record of completed work. Entries describing modules, APIs, or behavior that no
longer exist are marked superseded; see ARCHITECTURE.md for the current system and known
limitations. Deferred work is tracked in the external roadmap.

## Project 1 — Foundation & Hygiene
- [x] Central config (`SparkletConf`)
  - [x] Default shuffle partitions, default parallelism, thread pool size
  - [x] Inject into `StageBuilder` and `DAGScheduler`; remove magic `4`
- [x] Replace `println` with pluggable logging
  - [x] Likely `scala-logging` facade with `log4j` for async logging backend 
  - [x] Stage/Task/DAG logs with levels and simple timers
- [x] Stronger typing for IDs
  - [x] Newtypes: `StageId`, `ShuffleId`, `PartitionId`
  - [x] Refactor maps and method signatures to use them
- [x] Thread-safe `ShuffleManager`
  - [x] Use concurrent map with thread locks
  - [x] Re-enable `Test / parallelExecution := true` when safe
  - SUPERSEDED: tests stay sequential while `SparkletConf`/`SparkletRuntime` are process-global;
    parallelism returns with dependency injection (deferred)
- [x] Union correctness
  - [x] Implement union as true concatenation of inputs (not “pick left”)
- [x] Explicit join/cogroup inputs
  - [x] Carry explicit left/right `ShuffleId`s through `StageInfo.inputSources`
  - [x] Remove heuristic lookup in `DAGScheduler` for join/cogroup
- [x] Join semantics
  - [x] Implement correct inner join: cartesian product for matching keys (not head-only)
- [x] Remove stage ID as planned shuffle ID coupling
  - [x] Always use `ShuffleManager.write…` return as the real `ShuffleId`, no more relying on latest shuffle heuristic
  - [x] Maintain `stageId -> shuffleId` mapping explicitly
  - [x] Enable Shuffle IDs to only be touched by runtime, fully decoupled from query planning

## Project 2 — Extensibility & Module Boundaries
- [x] Define runtime and shuffle SPIs
  - [x] `TaskScheduler[F[_]]`, `ShuffleService`, `Partitioner`
  - [x] DAG scheduler depends only on SPIs
  - SUPERSEDED: `ExecutorBackend` removed in Milestone 2 — it was registered in
    `RuntimeComponents` but never called
- [x] Hide current implementations behind SPIs
  - [x] `runtime-local`: thread pool scheduler/executor
  - [x] `shuffle-local`: in-memory shuffle storage
- [x] Multi-module sbt reorg
  - SUPERSEDED module names: the build defines five modules — `sparklet-api`, `sparklet-core`,
    `sparklet-execution`, `sparklet-runtime`, `sparklet-tests`. The planner and shuffle work
    landed inside `sparklet-execution` and `sparklet-runtime` respectively; no
    `sparklet-dataset` module exists.

## Project 3 — Execution Correctness & Performance
- [x] Iterator-based execution
  - [x] Replace eager `.toSeq`/materialization in operators with streaming `Iterator`
  - Notes: narrow ops stream via LazyList-backed Iterables; shuffle boundaries still materialize. `LocalTaskScheduler` eagerly realizes task outputs to keep timing semantics in tests.
- [x] Partitioner metadata propagation
  - [x] Carry `Partitioner` through plans to avoid unnecessary reshuffles
  - [x] Add `repartition`, `coalesce`, `mapPartitions`, `partitionBy`
- [x] Global sort pipeline
  - [x] Sampling + range partitioner
  - [x] Repartition by ranges, local sort per partition, streaming merge on read
- [x] Join strategies
  - [x] Broadcast-hash Join (BHJ) when one side is small (config threshold)
  - [x] Hash/merge join selection hooks (basic heuristics)
  - [x] Shuffle-Hash Join (SHJ)
  - [x] Sort-Merge Join (SMJ)
  - [x] Add tests to cover all join strategies, and test automatic join strategy decisions
  - NOTE: the sort-merge join implementation groups by key with a hash-code-based ordering
    rather than consuming a real sort order; correctness is by key equality, not by merge.
    A true typed sort-merge join is deferred.

## Project 4 - Hygiene, Fault Tolerance, and Reliability

### Phase 1: Basic Retry Logic
- [x] `SparkletConf` fault tolerance settings: `maxTaskRetries`, `baseRetryDelayMs`,
  `maxRetryDelayMs` (wired into `LocalTaskScheduler.submit` as of Milestone 1)
  - SUPERSEDED conf fields: `enableLineageRecovery`, `taskTimeoutMs`,
    `enableSpeculativeExecution`, `speculativeExecutionThreshold` removed in Milestone 1 —
    they controlled nothing
- [x] `RetryPolicy` trait with exponential backoff implementation
- [x] `TaskScheduler[F[_]]` interface with `submitWithRetry` method
- [x] Comprehensive retry policy tests with edge cases

### Phase 2: Enhanced Retry & Recovery
- [x] `TaskExecutionWrapper[F[_]]` with retry logic
  - [x] Configurable retry policies with exponential backoff
  - [x] Detailed logging for retry attempts and failures
  - SUPERSEDED: `executeSimple`, `executeWithLineage`, and the `withRecovery` factory removed
    in Milestone 2; the wrapper is retry-only
- [x] `LineageRecoveryManager[F[_]]` for dependency-based recovery
  - REMOVED in Milestone 2: recovery could not safely reconstruct arbitrary user functions
    (it returned identity-like results) and was unreachable from the supported path
- [x] `LocalTaskScheduler` with fault tolerance integration
  - [x] Semaphore-based bounded parallelism
  - SUPERSEDED: `enableRecovery` constructor flag removed in Milestone 1; submission now
    applies the `SparkletConf`-derived retry policy at submission time
- [x] `Task` trait lineage support (`LineageInfo`, `TaskResult`)
  - REMOVED in Milestone 2

## Phase 1 Execution Cleanup (2026-09)
- [x] Milestone 1: contract tests and safe correctness fixes (#10)
  - `aggregate` honors `combOp` via partition-preserving `executePartitions`
  - source construction produces exactly `numPartitions` partitions (empty input safe)
  - `HashPartitioner` uses floorMod (negative hashes safe)
  - `LocalTaskScheduler.submit` applies the configured retry policy
  - legacy driver-collecting `reduceByKeyAction`/`groupByKeyAction` removed
- [x] Milestone 2: one DAG execution path, legacy deletion (#11)
  - every plan compiles to a stage graph; `Executor.createTasks`, `StageBuilder.buildStages`,
    the legacy adapter, `Task.DAGTask`, and the ten single-op task classes removed
  - `TopologicalSort` fixed to include stages without dependency edges
  - recovery subsystem removed; `TaskExecutionWrapper` retry-only
  - `take(n)` per-partition limit; `take(n <= 0)` returns empty
- [x] Milestone 3: WideOp migration, explicit shuffle policy (#13)
  - `StageInfo.wideOp: Option[WideOp]`; runtime dispatch no longer inspects `Plan`
  - single `Operation.fromPlan` erasure boundary
  - `ShuffleWriteReason` ADT with pure, tested priority selection
  - `DistCollection` KV casts collapsed into one `kvPlan` boundary
- [x] Milestone 4: enforcement and documentation (#14, this PR)
  - graph validation independently testable, every rejection mode covered by tests
  - `Wart.AsInstanceOf` and `Wart.Any` are compile errors; casts only at named boundaries
  - docs rewritten to describe the single-path architecture
