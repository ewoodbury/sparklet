# DAG Scheduler Data Flow Diagrams

## Overall Architecture

```mermaid
graph TD
    subgraph "Legacy Single-Stage Architecture"
        A1[DistCollection.collect] --> B1[Executor.createTasks]
        B1 --> C1[StageTask Creation]
        C1 --> D1[TaskScheduler.submit]
        D1 --> E1[Results]
        
        F1[Used for:<br/>- Narrow transformations only<br/>- map, filter, flatMap, etc.<br/>- Single execution stage]
    end
    
    subgraph "Integrated DAG Scheduler Architecture"
        A2[DistCollection.collect] --> B2[Executor.createTasks]
        B2 --> C2{Contains Shuffle?}
        C2 -->|No| D2[StageTask Path]
        C2 -->|Yes| E2[DAGTask Creation]
        
        E2 --> F2[DAGTask.run]
        F2 --> G2[DAGScheduler.execute]
        G2 --> H2[StageBuilder.buildStageGraph]
        H2 --> I2[Multi-Stage Coordination]
        I2 --> J2[ShuffleManager Integration]
        J2 --> K2[Final Results]
        
        L2[Capabilities:<br/>- All wide transformations<br/>- groupByKey, reduceByKey, sortBy<br/>- join, cogroup operations<br/>- Multi-stage dependencies<br/>- Shuffle data coordination]
    end
```

## Task Integration Architecture

```mermaid
graph TD
    subgraph "Executor.createTasks() Decision Logic"
        A[Plan Input] --> B{DAGScheduler.requiresDAGScheduling?}
        B -->|false| C[Narrow Transformations Only]
        B -->|true| D[Contains Shuffle Operations]
        
        C --> E[StageBuilder.buildStages]
        E --> F[StageTask Creation]
        F --> G[One StageTask per Partition]
        
        D --> H[DAGTask Creation] 
        H --> I[Single DAGTask for Entire Plan]
        I --> J[DAGTask.run → DAGScheduler.execute]
    end
    
    subgraph "Task Execution"
        G --> K[TaskScheduler.submit StageTask]
        J --> L[Multi-Stage DAG Execution]
        
        K --> M[Parallel Single-Stage Results]
        L --> N[Multi-Stage Coordinated Results]
        
        M --> O[Final Output]
        N --> O
    end
```

## Stage Graph Construction

```mermaid
graph TB
    subgraph "Input Plan Tree"
        A[MapOp: map result aggregation] 
        A --> B[GroupByKeyOp: shuffle boundary]
        B --> C[MapOp: data transformation]
        C --> D[Source: original data]
    end
    
    subgraph "Stage Graph Construction"
        E[StageBuilder.buildStageGraph] --> F{Plan Analysis}
        F -->|Narrow Ops| G[Chain into current stage]
        F -->|Shuffle Ops| H[Create new stage boundary]
        
        H --> I[Update Dependencies Map]
        I --> J[Assign Stage IDs]
    end
    
    subgraph "Result: Multi-Stage Graph"
        K[Stage 0: Source + Map Transform]
        L[Stage 1: GroupByKey + Map Aggregate]
        K --> M[Shuffle Data]
        M --> L
        
        N[Dependencies:<br/>Stage 1 depends on Stage 0]
    end
```

## Detailed Execution Flow

```mermaid
sequenceDiagram
    participant User as DistCollection
    participant Exec as Executor
    participant DAGTask as DAGTask
    participant DAG as DAGScheduler  
    participant SB as StageBuilder
    participant TS as TaskScheduler
    participant SM as ShuffleManager
    
    User->>Exec: collect() with shuffle operations
    Exec->>DAGTask: createTasks() → DAGTask(plan)
    DAGTask->>DAG: run() → execute(plan)
    DAG->>SB: buildStageGraph(plan)
    SB-->>DAG: StageGraph{stages, dependencies, finalStageId}
    
    DAG->>DAG: topologicalSort(dependencies)
    Note over DAG: Execution order: [0, 1, 2]
    
    loop For each stage in order
        DAG->>DAG: getInputPartitionsForStage()
        alt Stage 0 (Source Stage)
            DAG->>TS: submit(tasks for source data)
            TS-->>DAG: Partition results
        else Stage N (Shuffle Stage)  
            DAG->>SM: readShufflePartition(shuffleId, partitionId)
            SM-->>DAG: Shuffled partition data
            DAG->>TS: submit(shuffle tasks)
            TS-->>DAG: Processed results
        end
        
        alt Is Shuffle Stage
            DAG->>SM: partitionByKey(results)
            DAG->>SM: writeShuffleData(shuffledData)
            SM-->>DAG: shuffleId for next stage
        end
    end
    
    DAG-->>DAGTask: Final stage results
    DAGTask-->>User: Complete results
```

## Supported Shuffle Operations

### GroupByKey Operation
```mermaid
graph TD
    subgraph "Input: DistCollection(('a',1), ('b',2), ('a',3))"
        A["Original Data:<br/>Seq(('a',1), ('b',2), ('a',3))"]
    end
    
    subgraph "Stage 0: Source Stage"
        A --> B["Partition 0: ('a',1), ('b',2)<br/>Partition 1: ('a',3)"]
        B --> C["Identity Task (no transformation needed)"]
        C --> D["Stage 0 Output:<br/>('a',1), ('b',2), ('a',3)"]
    end
    
    subgraph "Shuffle Boundary"
        D --> E["ShuffleManager.partitionByKey"]
        E --> F["Hash Partition by Key"]
        F --> G["Shuffle Data Stored"]
        
        G --> H["Partition 0: key='a'<br/>(('a',1), ('a',3))"]
        G --> I["Partition 1: key='b'<br/>(('b',2))"]
    end
    
    subgraph "Stage 1: GroupByKey Operation"
        H --> J["GroupByKey Task 0<br/>Process key='a'"]
        I --> K["GroupByKey Task 1<br/>Process key='b'"] 
        
        J --> L["Result: ('a', [1, 3])"]
        K --> M["Result: ('b', [2])"]
    end
    
    subgraph "Final Output"
        L --> N["Combined Results:<br/>('a', [1, 3]), ('b', [2])"]
        M --> N
    end
```

### Join Operation
```mermaid
graph TD
    subgraph "Input: left.join(right)"
        A["Left: Seq(('a',1), ('b',2))<br/>Right: Seq(('a',3), ('b',4))"]
    end
    
    subgraph "Stage 0: Left Source" 
        A --> B["Left Partition: ('a',1), ('b',2)"]
        B --> C["Stage 0 Output → Shuffle ID 0"]
    end
    
    subgraph "Stage 1: Right Source"
        A --> D["Right Partition: ('a',3), ('b',4)"] 
        D --> E["Stage 1 Output → Shuffle ID 1"]
    end
    
    subgraph "Shuffle Coordination"
        C --> F["Left Shuffle Storage"]
        E --> G["Right Shuffle Storage"]
        
        F --> H["Join Stage Reads Both"]
        G --> H
    end
    
    subgraph "Stage 2: Join Operation"
        H --> I["Read Left Data: ('a',1), ('b',2)"]
        H --> J["Read Right Data: ('a',3), ('b',4)"]
        
        I --> K["Inner Join by Key"]
        J --> K
        
        K --> L["Result: ('a', (1,3)), ('b', (2,4))"]
    end
```

## ShuffleManager Operations

```mermaid
graph TD
    subgraph "ShuffleManager Components"
        A["shuffleStorage: Map[Int, ShuffleData]"]
        B["nextShuffleId: AtomicInteger"]
        C["ShuffleData: Map[PartitionId, Seq[(K,V)]]"]
    end
    
    subgraph "Write Phase: Stage Output"
        D["Stage produces key-value data"] --> E["partitionByKey(data, numPartitions)"]
        E --> F["Hash each key to partition:<br/>partition = key.hashCode % numPartitions"]
        F --> G["Group data by target partition"]
        G --> H["writeShuffleData(shuffleData)"]
        H --> I["Assign unique shuffleId"]
        I --> J["Store in shuffleStorage"]
    end
    
    subgraph "Read Phase: Next Stage Input"
        K["Stage needs shuffled input"] --> L["readShufflePartition(shuffleId, partitionId)"]
        L --> M["Lookup in shuffleStorage[shuffleId]"]
        M --> N["Extract data for partitionId"]
        N --> O["Return as Partition[(K,V)]"]
    end
    
    subgraph "Multi-Shuffle Join Support"
        P["Join Stage"] --> Q["Read from multiple shuffle IDs"]
        Q --> R["Left: readShufflePartition(shuffleId=0, partitionId)"]
        Q --> S["Right: readShufflePartition(shuffleId=1, partitionId)"]
        R --> T["Separate left/right data for join"]
        S --> T
    end
```

## Complete E2E Execution with Examples

### Example 1: GroupByKey
```mermaid
graph TB
    subgraph "Plan Construction"
        A["DistCollection(('a',1), ('a',3), ('b',2))"] --> B["groupByKey()"]
        B --> C["Plan.GroupByKeyOp(Source(...))"]
    end
    
    subgraph "Execution Decision"
        C --> D["Executor.createTasks()"]
        D --> E["DAGScheduler.requiresDAGScheduling = true"]
        E --> F["DAGTask(plan)"]
    end
    
    subgraph "Multi-Stage Execution"
        F --> G["DAGTask.run()"]
        G --> H["DAGScheduler.execute(plan)"]
        H --> I["2 stages: Source + GroupByKey"]
        I --> J["Stage 0: Data collection"]
        J --> K["Shuffle: partition by key"]
        K --> L["Stage 1: Group values by key"]
        L --> M["Result: ('a',[1,3]), ('b',[2])"]
    end
```

### Example 2: Complex Pipeline
```scala
// User code:
val result = DistCollection(data)
  .map(transform)      // Narrow - Stage chaining
  .filter(predicate)   // Narrow - Stage chaining  
  .groupByKey()        // Wide - Shuffle boundary
  .mapValues(aggregate) // Narrow - New stage after shuffle
  .collect()
```

```mermaid
graph LR
    A[Source Data] --> B["Stage 0:<br/>map + filter"]
    B --> C[Shuffle<br/>Boundary]
    C --> D["Stage 1:<br/>groupByKey"]
    D --> E["Stage 2:<br/>mapValues"]
    E --> F[Final Result]
    
    style C fill:#ffebee,stroke:#c62828
    style D fill:#e8f5e8,stroke:#388e3c
    style E fill:#e8f5e8,stroke:#388e3c
```

## Performance Characteristics

### Single-Stage Path (Narrow Operations)
- **Memory**: Minimal - operations pipelined in single pass
- **Network**: None - no data movement between partitions  
- **Parallelism**: High - each partition processes independently
- **Latency**: Low - direct execution path

### Multi-Stage Path (Wide Operations)  
- **Memory**: Moderate - shuffle data temporarily stored
- **Network**: Shuffle data redistribution between stages
- **Parallelism**: High - stages execute in parallel where possible
- **Latency**: Higher - coordination overhead and shuffle I/O
- **Throughput**: Optimized through stage pipelining

## Implementation Status

| Operation | Status | Notes |
|-----------|--------|-------|
| `groupByKey` | Fully Implemented | Groups values by key with shuffle |
| `reduceByKey` | Fully Implemented | Reduces values by key with shuffle |
| `sortBy` | Fully Implemented | Sorts elements by key function |
| `join` | Fully Implemented | Inner joins with dual shuffle read |
| `cogroup` | Implemented | Basic co-group functionality |
| Multi-stage coordination | Fully Operational | DAGScheduler handles dependencies |
| Shuffle management | Fully Operational | Hash partitioning and storage |
| Task integration | Complete | Seamless DAGTask → DAGScheduler routing |