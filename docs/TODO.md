# Sparklet TODOs (Prioritized)

## P0 — Foundation & Hygiene (now)
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

## P1 — Extensibility & Module Boundaries (next)
- [ ] Define runtime and shuffle SPIs
  - [ ] `TaskScheduler[F[_]]`, `ExecutorBackend`, `ShuffleService`, `Partitioner`
  - [ ] DAG scheduler depends only on SPIp
- [ ] Hide current implementations behind SPIs
  - [ ] `runtime-local`: thread pool scheduler/executor
  - [ ] `shuffle-local`: in-memory shuffle storage
- [ ] Multi-module sbt reorg
  - [ ] `sparklet-core` (Plan, DistCollection, model, config)
  - [ ] `sparklet-planner` (StageBuilder, future optimizer)
  - [ ] `sparklet-runtime-api`, `sparklet-runtime-local`
  - [ ] `sparklet-shuffle-api`, `sparklet-shuffle-local`
  - [ ] `sparklet-dataset` (typed API stub)
  - [ ] Update `build.sbt` aggregates/dependsOn

## P1 — Execution Correctness & Performance (next)
- [ ] Iterator-based execution
  - [ ] Replace eager `.toSeq`/materialization in operators with streaming `Iterator`
- [ ] Partitioner metadata propagation
  - [ ] Carry `Partitioner` through plans to avoid unnecessary reshuffles
  - [ ] Add `repartition`, `coalesce`, `mapPartitions`
- [ ] Global sort pipeline
  - [ ] Sampling + range partitioner
  - [ ] Repartition by ranges, local sort per partition, streaming merge on read
- [ ] Join strategies
  - [ ] Broadcast-hash join when one side is small (config threshold)
  - [ ] Hash/merge join selection hooks (basic heuristics)

## P2 — Advanced Features (later)
- [ ] Caching & checkpointing
  - [ ] `.persist()` with storage levels (memory/disk)
  - [ ] `.checkpoint()` to truncate lineage
- [ ] Failure/retry semantics
  - [ ] Per-task retry policy; deterministic recompute from lineage
- [ ] Metrics & observability
  - [ ] Counters, timers per stage/shuffle; simple reporter
  - [ ] Structured event log for executions
- [ ] Dataset API (align with user preference for typed APIs)
  - [ ] `Encoder[T]` typeclass; `Dataset[T]` with typed ops
  - [ ] Conversions between `DistCollection`, `Dataset`
- [ ] Physical plan + simple optimizer
  - [ ] `PhysicalPlan` nodes (e.g., `ShuffleExchange`, `LocalHashAggregate`, `SortMergeJoin`)
  - [ ] Basic rules: predicate pushdown, projection pruning, stage coalescing
- [ ] Pluggable serialization
  - [ ] Abstract serialization boundary; add efficient codecs
- [ ] Native/columnar runtime (exploratory)
  - [ ] `runtime-native` module with Arrow columnar batches and Panama bridge

## Test & CI
- [ ] Expand property/integration tests for joins, union, repartition, sortBy correctness
- [ ] Re-enable parallel tests after `ShuffleManager` is thread-safe
- [ ] CI pipeline: format, lint, test matrix for modules

## Nice-to-haves
- [ ] Broadcast variables accumulator stubs (API-level only)
- [ ] Simple web UI stub for job/stage visualization

