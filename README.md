# sparklet

A data processing engine inspired by Spark, in pure functional Scala 3.

## Overview

I'm aiming to build a distributed data processing engine modeled after Apache Spark, with the following differences:
- Scala 3
- Type safety
- Functional purity (with Cats Effect)
- Clean architecture and software design from first principles

As the project matures, I plan to shift focus to optimizing performance. My stretch goal is to be twice as fast as Spark for small, simple jobs, and to match Spark's performance on large jobs.


## Roadmap

### Basic Transformations

- [x] Category I: Narrow transformations (map, filter, flatMap, distinct, union)
- [x] Category II: Actions (collect, count, take, first, reduce, fold, aggregate, forEach)
- [x] Category III: Key-value pairs (mapValues, keys, values)
- [x] Category IV: Wide Transformations (groupBy, reduceByKey, sortBy/orderBy)
- [x] Category V: Joins


### Task Processing Logic

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
- [x] Executor
  - The executor runs tasks, manages local state, and handles retries on failure

### Architecture
- [x] Clean Modularization
  - Separate modules for core abstractions, execution engine, and distributed system features
- [x] Effect System
  - Use Cats Effect for all side effects, making the code pure and testable
- [x] Type Safety
  - Use type-safe transformations and eliminate unsafe casts as much as possible
- [x] Service Provider Interface (SPI)
  - Define clear interfaces for components like Scheduler and ShuffleManager to allow swapping implementations


### Local to Distributed Processing

- [x] Local Mode
- [x] Partitions Support
- [x] Shuffles
- [x] Basic task retries using lineage recovery
- [ ] Simulated Cluster Mode using threads
- [ ] Basic Cluster Mode


### DataFrames and DataSets

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


### Distributed System Features

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
