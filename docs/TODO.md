# Sparklet Roadmap

## Project 4 - Hygiene, Fault Tolerance, and Reliability

### Phase 0: Architecture and Foundations
- [x] Unify StageBuilder between execution and planner modules and improve design
  - Why: Prevent divergence between two builders; single source of truth for stage graph semantics. Current: `modules/sparklet-execution/.../StageBuilder.scala`, `modules/sparklet-planner/.../StageBuilder.scala`.
  - Benefit: Easier feature evolution, less code drift, consistent optimization rules.
- [x] Single wide-op predicate (`PlanWide.isWide`) — remove duplicate shuffle detection logic
  - Why: Currently duplicated logic risks inconsistent scheduling decisions. Current detection scattered in `DAGScheduler.scala`, `StageBuilder.containsShuffleOperations`.
  - Benefit: Deterministic shuffle boundary detection & simpler maintenance.
- [x] Remove unsafe `isInstanceOf` casts in stage fusion logic
  - Why: Current reliance on runtime casts risks ClassCastException. Casts in `StageExecutor` (todo), `ShuffleHandler` (todo), `StageBuilder` (done).
  - Benefit: Safer, more maintainable code; easier reasoning about stage types.
- [ ] Enrich partition metadata (`PartitioningInfo`): partitioner, distribution type, ordering tag
  - Why: Present metadata (`byKey`, count) insufficient to decide shuffle reuse / ordering guarantees. Current minimal `Partitioning` in `StageBuilder.scala`.
  - Benefit: Enables shuffle reuse, join strategy selection, sort/order correctness checks.
- [ ] Eliminate legacy narrow-only path; route all execution through unified DAG scheduler
  - Why: Dual execution paths increase complexity & bug surface. Current legacy path in `Executor.createTasks` + `StageBuilder.buildStagesRecursiveOld`.
  - Benefit: Single tested path, simpler debugging, unlocks uniform metrics & recovery.
- [ ] Introduce typed `StageOp` / `StageChain` to remove `Any` + unsafe casts in stage fusion
  - Why: Current fusion relies on `asInstanceOf`, risking runtime ClassCast failures. Casts in `StageBuilder.buildStagesRecursive` & stage chaining helpers.
  - Benefit: Compile-time safety, easier reasoning about pipelines, better IDE support.
- [ ] Centralize shuffle write policy with explicit `ShuffleWriteReason` ADT for logging & recovery
  - Why: Shuffle emission rationale is implicit/scattered. Current logic fragments: `ExecutionPlanner.writeShuffleIfNeeded`, `StageExecutor` shuffle handling.
  - Benefit: Transparent diagnostics, future adaptive execution hook points.
- [ ] Strengthen `InputSource` typing (add `DataDescriptor` or parametric types) to reduce casts
  - Why: `InputSource` loses element type, forcing casts downstream. Current untyped ADT in `StageBuilder.scala`.
  - Benefit: Type-safe stage wiring, earlier error detection, cleaner executor code.
- [ ] Add dedicated physical stage kinds (`ShuffleJoinStage`, `GlobalSortStage`) instead of generic identity placeholders
  - Why: Wide ops currently represented by generic stages with hidden semantics. Current placeholders: shuffle stages created with generic `Stage[_,_]` in `StageBuilder.createShuffleStage`; join/sort logic partly in `StageExecutor`.
  - Benefit: Encapsulated execution logic, clearer metrics, pluggable strategies.
- [ ] Consolidate wide-op detection & shuffle decision utilities into a single module (planner)
  - Why: Related logic fragmented across scheduler, builder, planner. Current: `DAGScheduler.requiresDAGScheduling`, `StageBuilder.containsShuffleOperations`, planner heuristics.
  - Benefit: Central policy surface, easier optimization insertion (e.g., stage coalescing).
- [ ] Prepare physical plan abstraction layer (scaffold `PhysicalPlan` nodes) ahead of optimizer work
  - Why: Direct Plan->Stage mapping blocks future rule-based optimization. Current direct translation in `StageBuilder`; no `PhysicalPlan` layer yet.
  - Benefit: Enables projection/predicate pushdown, cost-based selection, adaptive re-planning.

### Phase 3: Advanced Recovery & Speculative Execution
- [x] Deterministic recompute from lineage on task failure
  - [x] Complete implementation of `LineageRecoveryManager.recoverFailedTask`
  - [x] Full integration with execution module for task reconstruction
  - [ ] Recovery of complex operations (joins, aggregations, etc.) (skip for now)
- [ ] Speculative execution for slow tasks (optional, will skip for now)
- [ ] Graceful handling of executor crashes
  - [ ] Task reassignment to healthy executors
  - [ ] Partial result preservation and continuation
- [ ] Advanced failure modes testing
  - [ ] Inject artificial failures in integration tests
  - [ ] Chaos engineering approach to fault tolerance validation
  - [ ] Performance impact analysis under failure conditions

### Phase 4: Production Hardening & Observability (FUTURE)
- [ ] Circuit breaker pattern for persistent failures
- [ ] Task execution metrics and monitoring
- [ ] Alerting for retry exhaustion and recovery failures
- [ ] Historical failure pattern analysis
- [ ] Resource-aware retry policies
- [ ] Dynamic retry configuration based on system load

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




