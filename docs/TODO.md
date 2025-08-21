# Sparklet Roadmap
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

## Project 4 — Hygiene

### Error Handling & Fault Tolerance
- [ ] Add retry logic to `TaskScheduler[F[_]]`
  - [ ] `submitWithRetry(tasks, maxRetries)` method
  - [ ] Configurable retry policies per operation type
  - [ ] Exponential backoff for transient failures
- [ ] Task-level error recovery
  - [ ] Deterministic recompute from lineage on task failure
  - [ ] Graceful handling of executor crashes
- [ ] Add failure modes testing
  - [ ] Inject artificial failures in tests
  - [ ] Verify lineage-based recovery works correctly

### Memory Management
- [ ] Memory-aware execution
  - [ ] Add `MemoryConf` to `SparkletConf`
  - [ ] Track partition sizes and prevent OOM
  - [ ] Implement spill-to-disk for large partitions
- [ ] Adaptive partitioning
  - [ ] Dynamic partition sizing based on data skew
  - [ ] Memory pressure detection and response

### Observability & Metrics
- [ ] Basic metrics collection
  - [ ] `ExecutionMetrics` case class with counters/timers
  - [ ] Stage completion times, shuffle bytes, task durations
  - [ ] Simple metrics reporter (console/file output)
- [ ] Structured event logging
  - [ ] JSON event log for query executions
  - [ ] Stage/task lifecycle events
- [ ] Performance monitoring
  - [ ] Memory usage tracking per stage
  - [ ] Shuffle read/write bandwidth monitoring

---

## Project 5 — User-Facing API Expansion

### DataFrame API (Priority 1)
- [ ] Core DataFrame implementation
  - [ ] `DataFrame(plan: Plan[Row], schema: StructType)`
  - [ ] Row representation and schema handling
  - [ ] Basic column expressions (`Column` class)
- [ ] Essential DataFrame operations
  - [ ] `select(cols: Column*)` - projection
  - [ ] `filter(condition: Column)` - row filtering  
  - [ ] `withColumn(name: String, col: Column)` - add/replace columns
  - [ ] `drop(colNames: String*)` - remove columns
- [ ] Aggregations and grouping
  - [ ] `groupBy(cols: Column*)` returning `GroupedData`
  - [ ] `agg()` with common functions: `sum`, `count`, `avg`, `max`, `min`
  - [ ] Window functions (basic implementation)
- [ ] DataFrame I/O
  - [ ] `DataFrame.read.parquet(path)` - read from files
  - [ ] `DataFrame.write.parquet(path)` - write to files
  - [ ] CSV and JSON support

### Dataset API (Priority 2)  
- [ ] Encoder system
  - [ ] `Encoder[T]` typeclass for case class serialization
  - [ ] Built-in encoders for primitives and common types
  - [ ] Automatic derivation for case classes
- [ ] Core Dataset implementation
  - [ ] `Dataset[T](plan: Plan[T], encoder: Encoder[T])`
  - [ ] Type-safe transformations: `map[U]`, `filter`, `flatMap`
  - [ ] Typed aggregations: `groupByKey`, `reduceGroups`
- [ ] Dataset-DataFrame interop
  - [ ] `Dataset.toDF()` conversion
  - [ ] `DataFrame.as[T]()` conversion with encoders

### API Integration & Testing
- [ ] Comprehensive API tests
  - [ ] End-to-end DataFrame workflows
  - [ ] Type safety verification for Dataset operations
  - [ ] Performance benchmarks vs DistCollection API
- [ ] Documentation and examples
  - [ ] API documentation with examples
  - [ ] Migration guide from DistCollection to DataFrame/Dataset
  - [ ] Sample ETL job implementations

---

## Project 6 — Distributed Execution

### Network Communication Layer
- [ ] Driver-Executor communication
  - [ ] gRPC service definitions for task submission
  - [ ] `DriverService` and `ExecutorService` traits
  - [ ] Protobuf schemas for task serialization
- [ ] Task serialization
  - [ ] `SerializedTask` with portable operation chains
  - [ ] Function serialization for UDFs
  - [ ] Partition data serialization protocols

### Standalone Executor JVM
- [ ] Lightweight executor process
  - [ ] Minimal JVM that receives and executes tasks
  - [ ] Resource management (CPU, memory limits)
  - [ ] Heartbeat and health reporting to driver
- [ ] Executor lifecycle management
  - [ ] Startup, task execution, graceful shutdown
  - [ ] Dynamic resource allocation
  - [ ] Failure detection and recovery

### Distributed Shuffle Service
- [ ] Network-based shuffle
  - [ ] Extend `ShuffleService` for remote storage
  - [ ] Shared storage backend (S3/HDFS) for shuffle data
  - [ ] Efficient shuffle read/write over network
- [ ] Shuffle optimization
  - [ ] Compression for shuffle data
  - [ ] Local disk caching of remote shuffle blocks
  - [ ] Shuffle service fault tolerance

### Cluster Resource Management
- [ ] Kubernetes-based deployment
  - [ ] `ClusterManager[F[_]]` trait for executor lifecycle
  - [ ] Kubernetes job/pod management for executors
  - [ ] Dynamic scaling based on workload
- [ ] Resource allocation
  - [ ] Request executors based on job requirements
  - [ ] Resource quotas and limits
  - [ ] Multi-tenant resource sharing

### Distributed Runtime Implementation
- [ ] `sparklet-runtime-cluster` module
  - [ ] Distributed implementations of core SPIs
  - [ ] Network-aware task scheduling
  - [ ] Cluster-wide resource coordination
- [ ] Configuration and deployment
  - [ ] Cluster configuration management
  - [ ] Docker images for driver and executor
  - [ ] Helm charts for Kubernetes deployment

---

## Project 7 — Advanced Features (All TBD)

### Caching & Persistence
- [ ] In-memory caching
  - [ ] `.persist()` with storage levels (memory/disk/both)
  - [ ] LRU eviction policies
  - [ ] Memory-aware cache management
- [ ] Checkpointing
  - [ ] `.checkpoint()` to truncate lineage
  - [ ] Reliable storage for checkpoint data
  - [ ] Automatic checkpoint placement optimization

### Query Optimization
- [ ] Physical plan layer
  - [ ] `PhysicalPlan` nodes (`ShuffleExchange`, `LocalHashAggregate`, etc.)
  - [ ] Cost-based optimizer with statistics
  - [ ] Rule-based optimizations
- [ ] Advanced optimizations
  - [ ] Predicate pushdown through joins/aggregations
  - [ ] Projection pruning for columnar formats
  - [ ] Stage coalescing and pipeline optimization
  - [ ] Adaptive query execution

### Advanced I/O & Formats
- [ ] Pluggable serialization
  - [ ] Abstract serialization boundary
  - [ ] Kryo, Avro, Protocol Buffers support
  - [ ] Schema evolution handling
- [ ] Columnar execution (Exploratory)
  - [ ] Apache Arrow integration
  - [ ] `runtime-native` module with Panama bridge
  - [ ] Vectorized operations for numeric data

### Streaming & Real-time
- [ ] Streaming API foundation
  - [ ] `StreamingDataFrame` for continuous processing
  - [ ] Windowing and watermark support
  - [ ] Kafka source/sink connectors
- [ ] Incremental computation
  - [ ] Delta processing for batch jobs
  - [ ] Change data capture (CDC) support

---

## Project 8 — Testing/QA

### Comprehensive Testing
- [ ] Property-based testing
  - [ ] ScalaCheck tests for transformation correctness
  - [ ] Invariant testing across all join types
  - [ ] Roundtrip testing for serialization
- [ ] Integration testing
  - [ ] End-to-end job execution tests
  - [ ] Multi-node cluster testing
  - [ ] Failure injection and recovery testing
- [ ] Performance testing
  - [ ] Benchmark suite for core operations
  - [ ] Scalability testing with increasing data sizes
  - [ ] Memory usage and GC pressure analysis

### CI/CD Pipeline
- [ ] Automated testing
  - [ ] Multi-module test execution
  - [ ] Cross-platform testing (Linux/macOS)
  - [ ] Performance regression detection
- [ ] Code quality
  - [ ] Scalafmt, Wartremover, Scalafix integration
  - [ ] Test coverage reporting
  - [ ] Documentation generation

---


## Project 9 — DataFusion (TBD)

### DataFusion Bridge Architecture
- [ ] Design JNI bridge interface
  - [ ] `DataFusionExecutor[F[_]]` trait for native execution
  - [ ] Arrow-based data interchange (`ArrowBatch`, `ArrowSchema`)
  - [ ] Plan translation layer: Sparklet Plan → DataFusion LogicalPlan
- [ ] Native execution module
  - [ ] `sparklet-datafusion-native` Rust crate
  - [ ] JNI bindings for plan execution
  - [ ] Arrow IPC serialization for JVM↔Rust data transfer
- [ ] Scala bridge implementation
  - [ ] `sparklet-datafusion-bridge` module
  - [ ] `JNIDataFusionExecutor` with error handling
  - [ ] Arrow decoders/encoders for common types

### Plan Translation Layer
- [ ] Logical plan translation
  - [ ] `PlanTranslator` trait for Sparklet → DataFusion conversion
  - [ ] Support for common operations: filter, map, join, aggregation
  - [ ] Handle UDFs and custom functions
- [ ] Physical plan optimization
  - [ ] Leverage DataFusion's optimizer for vectorized execution
  - [ ] Predicate pushdown and projection pruning
  - [ ] Columnar batch processing for numerical operations
- [ ] Schema management
  - [ ] Arrow schema inference from Scala types
  - [ ] Type-safe conversions between Row/case classes and Arrow records

### Native Performance Features
- [ ] Vectorized execution
  - [ ] SIMD-optimized operations for numerical data
  - [ ] Columnar batch processing (configurable batch sizes)
  - [ ] Memory-efficient string and date operations
- [ ] Memory management
  - [ ] Off-heap Arrow buffers to reduce GC pressure
  - [ ] Zero-copy data transfer where possible
  - [ ] Configurable memory pools for large datasets
- [ ] Advanced SQL support
  - [ ] Window functions, CTEs, complex expressions
  - [ ] Subquery optimization and execution
  - [ ] SQL→DataFrame compilation pipeline

### Integration Strategy
- [ ] Hybrid execution model
  - [ ] Keep existing Scala execution for development/testing
  - [ ] DataFusion execution for performance-critical workloads
  - [ ] Runtime switching via configuration flags
- [ ] Compatibility layer
  - [ ] Maintain existing DistCollection/DataFrame APIs
  - [ ] Transparent acceleration for supported operations
  - [ ] Graceful fallback to Scala execution for unsupported features
- [ ] Performance benchmarking
  - [ ] Benchmark suite comparing Scala vs DataFusion execution
  - [ ] Memory usage and GC pressure analysis
  - [ ] Vectorization effectiveness for different data types

---

## Other Ideas

- [ ] Web UI for job monitoring
  - [ ] Stage execution visualization
  - [ ] Task timeline and resource usage
  - [ ] Query plan visualization
- [ ] Advanced APIs
  - [ ] Broadcast variables with automatic distribution
  - [ ] Accumulators for custom metrics
  - [ ] ML pipeline integration hooks
- [ ] Ecosystem integration
  - [ ] Jupyter notebook support
  - [ ] IDE integration and debugging tools
  - [ ] Third-party tool connectors




