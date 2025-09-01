# Completed Todos
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
  - [x] `TaskScheduler[F[_]]`, `ExecutorBackend`, `ShuffleService`, `Partitioner`
  - [x] DAG scheduler depends only on SPIs
- [x] Hide current implementations behind SPIs
  - [x] `runtime-local`: thread pool scheduler/executor
  - [x] `shuffle-local`: in-memory shuffle storage
- [x] Multi-module sbt reorg
  - [x] `sparklet-core` (Plan, DistCollection, model, config)
  - [x] `sparklet-planner` (StageBuilder, future optimizer)
  - [x] `sparklet-runtime-api`, `sparklet-runtime-local`
  - [x] `sparklet-shuffle-api`, `sparklet-shuffle-local`
  - [x] `sparklet-dataset` (typed API stub)
  - [x] Update `build.sbt` aggregates/dependsOn

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

## Project 4 - Hygiene, Fault Tolerance, and Reliability

### Phase 1: Basic Retry Logic
- [x] Enhanced `SparkletConf` with fault tolerance settings:
  - `maxTaskRetries`: Maximum retry attempts (default: 3)
  - `baseRetryDelayMs`: Base delay for exponential backoff (default: 10ms)
  - `maxRetryDelayMs`: Maximum delay cap (default: 1000ms)
  - `enableLineageRecovery`: Toggle for lineage-based recovery (default: true)
  - `taskTimeoutMs`: Task execution timeout (default: 30000ms)
  - `enableSpeculativeExecution`: Toggle for speculative execution (default: false)
  - `speculativeExecutionThreshold`: Slow task threshold (default: 2.0x)
- [x] `RetryPolicy` trait with exponential backoff implementation
- [x] Enhanced `TaskScheduler[F[_]]` interface with `submitWithRetry` method
- [x] Comprehensive retry policy tests with edge cases

### Phase 2: Enhanced Retry & Recovery
- [x] `TaskExecutionWrapper[F[_]]` with integrated retry logic
  - [x] Configurable retry policies with exponential backoff
  - [x] Task execution timeout handling
  - [x] Detailed logging for retry attempts and failures
- [x] `LineageRecoveryManager[F[_]]` for dependency-based recovery
  - [x] Framework for lineage-based task recovery
  - [x] Integration with shuffle service for data reconstruction
  - [x] Recovery statistics and monitoring
- [x] Enhanced `LocalTaskScheduler` with fault tolerance integration
  - [x] Lazy initialization of execution wrapper and recovery manager
  - [x] Support for both simple and retry-enabled task execution
  - [x] Proper parallelism handling with semaphore-based task limits
- [x] Updated `Task` trait with optional lineage support
  - [x] `LineageInfo` case class for task metadata tracking
  - [x] `TaskResult` sealed trait for structured task outcomes
  - [x] Backward compatibility for existing task implementations
- [x] Comprehensive test coverage:
  - [x] `TestRetryPolicy`: 167 test cases covering all retry scenarios
  - [x] `TestTaskExecutionWrapper`: Retry logic and failure handling
  - [x] `TestTaskScheduler`: Concurrent execution with timing validation
  - [x] Integration tests for fault tolerance end-to-end workflows