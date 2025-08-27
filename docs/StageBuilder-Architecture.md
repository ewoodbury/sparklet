# StageBuilder Architecture

## Overview
The new unified StageBuilder replaces the legacy recursive approach with an operation accumulation model that provides better introspection, optimization, and future code generation capabilities.

## Core Components

### StageGraph Model
```scala
case class StageGraph(
  stages: Map[StageId, StageInfo],        // All stages in the graph
  dependencies: Map[StageId, Set[StageId]], // stage ID -> dependent stage IDs
  finalStageId: StageId                     // Entry point for execution
)
```

### StageInfo Structure
```scala
case class StageInfo(
  id: StageId,
  stage: Stage[_, _],                    // Materialized stage
  inputSources: Seq[InputSource],        // Where stage reads from
  isShuffleStage: Boolean,               // True for shuffle boundaries
  shuffleOperation: Option[Plan[_]],     // Original wide operation
  outputPartitioning: Option[Partitioning] // Metadata for optimizations
)
```

## InputSource Types

### SourceInput
- Reads from original data partitions
- Used for base data sources

### ShuffleInput
```scala
case class ShuffleInput(
  stageId: StageId,           // Upstream stage producing data
  side: Option[Side],         // Left/Right for multi-input ops
  numPartitions: Int          // Expected partition count
)
```

### StageOutput
- References runtime output of upstream stages
- Used for narrow operations and unions

## Operation Model

### Narrow Operations
- `MapOp`, `FilterOp`, `FlatMapOp`, `DistinctOp`
- `KeysOp`, `ValuesOp`, `MapValuesOp`, `FilterKeysOp`, `FilterValuesOp`
- `FlatMapValuesOp`, `MapPartitionsOp`
- Local variants: `GroupByKeyLocalOp`, `ReduceByKeyLocalOp`

### Wide Operations (Shuffle Boundaries)
- `GroupByKeyOp`, `ReduceByKeyOp`, `SortByOp`
- `PartitionByOp`, `RepartitionOp`, `CoalesceOp`
- `JoinOp`, `CoGroupOp`

## Partitioning Propagation Rules

| Operation Type | Preserves Partitioning | byKey Behavior | Notes |
|---------------|------------------------|----------------|-------|
| Map/Filter/FlatMap | Preserved | Preserved | No changes |
| Keys | Preserved | Set false | Key-only output |
| Values | Preserved | Set false | Value-only output |
| MapValues/FilterValues | Preserve | Preserve | Structure unchanged |
| Distinct | Preserve | Conditional | Clear if not byKey |
| GroupByKey/ReduceByKey | New | Set true | Creates key-based partitioning |
| SortBy | New | Set false | Range partitioning |
| PartitionBy | New | Set true | Explicit key partitioning |
| Repartition | New | Set false | Hash partitioning |
| Coalesce | New | Set false | Reduced partition count |
| Join/CoGroup | New | Set true | Key-based partitioning |

## WideOp Enumeration

```scala
enum WideOpKind:
  case GroupByKey, ReduceByKey, SortBy, PartitionBy
  case Repartition, Coalesce, Join, CoGroup
```

Each wide operation includes metadata:
- `numPartitions`: Target partition count
- `keyFunc`, `reduceFunc`: Operation-specific functions
- `joinStrategy`: Join algorithm selection
- `sides`: Input ordering for multi-input operations

## Guidelines for New Plan Operations

### Adding Narrow Operations
1. **Create Operation ADT**: Add case class in `Operation` sealed trait
2. **Implement Materialization**: Add pattern match in `materialize()` method
3. **Update Partitioning**: Define `updatePartitioning()` rule
4. **Add Tests**: Verify chaining and partitioning behavior

### Adding Wide Operations
1. **Create WideOp Case Class**: Extend `WideOp` trait
2. **Implement Shuffle Stage**: Add to `createShuffleStageUnified()` method
3. **Define Partitioning**: Set output partitioning based on operation semantics
4. **Add Side Markers**: For multi-input operations, ensure `Side.Left`/`Side.Right` assignment
5. **Update Tests**: Verify shuffle boundary detection and side tagging

### Key Design Principles
- **Immutable Accumulation**: Operations collected in `Vector[Operation]`
- **Late Materialization**: Convert to `Stage` forms only when needed
- **Explicit Side Tagging**: Multi-input operations use stable `Side` markers
- **Post-Build Validation**: Comprehensive invariant checking
- **Backward Compatibility**: Legacy API support during transition

## Architecture Benefits

1. **Better Introspection**: Operation vector provides clear execution plan
2. **Optimization Opportunities**: Partitioning metadata enables bypass optimizations
3. **Future-Proof**: Structure ready for code generation and advanced optimizations
4. **Clean Separation**: Public API minimal, implementation details encapsulated
5. **Validation**: Early error detection prevents runtime failures

## Migration Path

1. **Phase Complete**: Unified builder handles all Plan variants
2. **Legacy Support**: `buildStages()` provides backward compatibility
3. **Validation**: Post-build checks ensure correctness
4. **Documentation**: Architecture clearly defined for future development

