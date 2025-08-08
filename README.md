# sparklet

A data processing engine inspired by Spark, in pure functional Scala.


## Transformations

- [x] Category I: Narrow transformations (map, filter, flatMap, distinct, union)
- [x] Category II: Actions (collect, count, take, first, reduce, fold, aggregate, forEach)
- [x] Category III: Key-value pairs (mapValues, keys, values)
- [x] Category IV: Wide Transformations (groupBy, reduceByKey, sortBy/orderBy)
- [x] Category V: Joins


## Task Processing Logic

- [x] Lazy Processing with Plans
- [x] Tasks and Scheduler
  - A task is a unit of work (composed of one or more plans) that can run on executor
  - A scheduler assigns tasks to an executor to process
- [x] Stages
  - Collections of tasks that can be run in parallel without shuffles (e.g. a chain of narrow transformations)
- [x] DAG Scheduler
  - Takes a plan, finds shuffle boundaries, and builds a Stage dependency graph
- [x] Shuffle Management
  - Scheduler can automatically collect results, shuffle into new partitions, then kick off the next stage 


## From Local to Distributed Processing

- [x] Local Mode
- [x] Partitions Support
- [x] Shuffles
- [ ] Simulated Cluster Mode using threads
- [ ] Basic Cluster Mode


## DataFrames and DataSets

- [ ] DataFrame API
  - [ ] Implement untyped DataFrame abstraction (row-based, schema as metadata)
  - [ ] Basic DataFrame operations: select, filter, withColumn, drop, etc.
  - [ ] Support for simple column expressions and projections

- [ ] Dataset API
  - [ ] Implement strongly-typed Dataset abstraction (case class rows, compile-time type safety)
  - [ ] Typed transformations: map, flatMap, filter, groupBy, etc.
  - [ ] Encoder/Decoder system for case class <-> row conversion

- [ ] Interoperability
  - [ ] Conversion between DistCollection, DataFrame, and Dataset
  - [ ] Schema inference for case classes and collections

- [ ] Optimizations
  - [ ] Logical and physical plan optimizations for DataFrame/Dataset queries
  - [ ] Columnar execution support


## Distributed System Features

- [ ] Serialization 
  - Serialize data so it can be sent between machines
- [ ] Inter-Process Communication (IPC)
  - Driver can send metadata to and from executors via RPC
- [ ] Standalone Executors
  - The lightweight JVM program that actually runs on the worker machines
- [ ] Resource and Cluster Management
  - Driver can discover workers, manage failures, and be resilient
  - Likely built on top of Kubernetes
- [ ] Data Persistence
  - Read from and write to parquet files in object storage (S3)
  - Cache intermediate results to local disk
- [ ] Distributed Shuffle
  - Executors write to shared location on local disk, from where executors can read for downstream stages


## Improvements versus Spark

- True Functional Purity and Safety
  - Compile-time guarantees for all task, communication, and IO operations with a modern functional effect system (ZIO/Cats)
  - Better concurrency - using lightweight "fibers" instead of JVM threads
  - First class type safety for datasets: comprehensive support for strong compile time checks in all pipeline code

- Bare-Metal Performance
  - Removal of GC pauses on JVM by moving processing off-heap
  - Executors use native code (Rust/C++) with Project Panama and Apache Arrow and DataFusion
  - Vectorization - process data entire columns at a time, rather than row by row (Spark already does this via JVM heap)

- Streaming-First (unsure about this one so far)
  - Design for real-time stream processing use cases as the primary processing case. All operations (groupByKey, join) use stateful operators that continously update their results
  - Similar to Flink


- ML-Optimized Query Planning and Execution (also unsure)
  - Instead of using basic heuristics like Catalyst, use an ML model trained on past query performance. Many batch jobs run a very similar workload day after day -- we can take advantage of this to drastically improve performance after a few initial runs.
  - Truly Adaptive Execution: During a run, engine can adjust parallelism, increase resource limits, detect and fix skew, and take other corrective actions automatically.  