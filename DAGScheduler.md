# DAG Scheduler Data Flow Diagrams

## Overall Architecture
```mermaid
graph TD
    subgraph "Old Single-Stage Architecture"
        A1[DistCollection.collect] --> B1[Executor.createTasks]
        B1 --> C1[Single Stage Tasks]
        C1 --> D1[TaskScheduler.submit]
        D1 --> E1[Results]
        
        F1[Limitations:<br/>- Only narrow transformations<br/>- No shuffle support<br/>- Single execution stage]
    end
    
    subgraph "New DAG Scheduler Architecture"
        A2[DistCollection.collect] --> B2{Contains Shuffle?}
        B2 -->|No| C2[Legacy Single-Stage Path]
        B2 -->|Yes| D2[DAGScheduler.execute]
        
        D2 --> E2[StageBuilder.buildStageGraph]
        E2 --> F2[Stage Graph with Dependencies]
        F2 --> G2[Topological Sort]
        G2 --> H2[Execute Stages in Order]
        H2 --> I2[ShuffleManager coordination]
        I2 --> J2[Multi-Stage Results]
        
        K2[Capabilities:<br/>- Wide transformations<br/>- Shuffle operations<br/>- Multi-stage coordination<br/>- Stage dependencies]
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
    participant DC as DistCollection
    participant DAG as DAGScheduler  
    participant SB as StageBuilder
    participant TS as TaskScheduler
    participant SM as ShuffleManager
    
    DC->>DAG: collect() with shuffle operations
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
    
    DAG-->>DC: Final stage results
```

## Concrete Data Flow
```mermaid
graph TD
    subgraph "Input: source.map(x => (x._1, x._2.toUpperCase)).groupByKey"
        A["Original Data:<br/>Seq((1, a), (2, b), (1, c))"]
    end
    
    subgraph "Stage 0: Narrow Transformations"
        A --> B["Partition 0:<br/>((1, a), (2, b), (1, c))"]
        B --> C["Map Task: toUpperCase"]
        C --> D["Stage 0 Output:<br/>((1, A), (2, B), (1, C))"]
    end
    
    subgraph "Shuffle Boundary"
        D --> E["ShuffleManager.partitionByKey"]
        E --> F["Hash Partition by Key"]
        F --> G["Shuffle Data Stored"]
        
        G --> H["Partition 0: key=1<br/>((1, A), (1, C))"]
        G --> I["Partition 1: key=2<br/>((2, B))"]
    end
    
    subgraph "Stage 1: Wide Transformation"
        H --> J["GroupByKey Task 0<br/>Process key=1"]
        I --> K["GroupByKey Task 1<br/>Process key=2"] 
        
        J --> L["Result: (1, [A, C])"]
        K --> M["Result: (2, [B])"]
    end
    
    subgraph "Final Output"
        L --> N["Combined Results:<br/>(1, [A, C]), (2, [B])"]
        M --> N
    end
```

## Types of Stages 
```mermaid
graph TD
    subgraph "Input: source.map(x => (x._1, x._2.toUpperCase)).groupByKey"
        A["Original Data:<br/>Seq((1, a), (2, b), (1, c))"]
    end
    
    subgraph "Stage 0: Narrow Transformations"
        A --> B["Partition 0:<br/>((1, a), (2, b), (1, c))"]
        B --> C["Map Task: toUpperCase"]
        C --> D["Stage 0 Output:<br/>((1, A), (2, B), (1, C))"]
    end
    
    subgraph "Shuffle Boundary"
        D --> E["ShuffleManager.partitionByKey"]
        E --> F["Hash Partition by Key"]
        F --> G["Shuffle Data Stored"]
        
        G --> H["Partition 0: key=1<br/>((1, A), (1, C))"]
        G --> I["Partition 1: key=2<br/>((2, B))"]
    end
    
    subgraph "Stage 1: Wide Transformation"
        H --> J["GroupByKey Task 0<br/>Process key=1"]
        I --> K["GroupByKey Task 1<br/>Process key=2"] 
        
        J --> L["Result: (1, [A, C])"]
        K --> M["Result: (2, [B])"]
    end
    
    subgraph "Final Output"
        L --> N["Combined Results:<br/>(1, [A, C]), (2, [B])"]
        M --> N
    end
```

## ShuffleManager
```mermaid
graph TD
    subgraph "ShuffleManager Components"
        A["shuffleStorage: Map[Int, ShuffleData]"]
        B["nextShuffleId: Counter"]
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
    
    subgraph "Hash Partitioning Example"
        P["Key 'apple' → hash → partition 0"]
        Q["Key 'banana' → hash → partition 1"] 
        R["Key 'cherry' → hash → partition 0"]
        S["Result: Partition 0: [apple, cherry]<br/>Partition 1: [banana]"]
    end
```

## Complete E2E Execution
```mermaid
graph TB
    subgraph "Plan Construction"
        A["DistCollection transformations"] --> B["Build Plan tree"]
        B --> C["Plan contains shuffle ops?"]
    end
    
    subgraph "Execution Decision"
        C -->|No| D["Legacy single-stage path"]
        C -->|Yes| E["DAGScheduler.execute()"]
    end
    
    subgraph "Stage Graph Building"
        E --> F["StageBuilder traverses Plan"]
        F --> G["Identify shuffle boundaries"]
        G --> H["Create StageInfo objects"]
        H --> I["Build dependency graph"]
        I --> J["Return StageGraph"]
    end
    
    subgraph "Execution Coordination"
        J --> K["Topological sort stages"]
        K --> L["Initialize stageResults storage"]
        L --> M["Execute stages in order"]
    end
    
    subgraph "Per-Stage Execution"
        M --> N["Get input partitions"]
        N --> O["Create tasks for stage"]
        O --> P["TaskScheduler executes tasks"]
        P --> Q["Store stage results"]
        Q --> R["Handle shuffle output if needed"]
        R --> S["Continue to next stage"]
    end
    
    subgraph "Final Result"
        S --> T["All stages complete"]
        T --> U["Extract final stage results"]
        U --> V["Return to DistCollection.collect()"]
    end
```