# Architecture

## Processing Core

The core processing engine is composed of three layers: a logical planning layer, a task layer, and a DistCollection layer.

### 1. Planning Layer

- Lazy, immutable representation of what will be computed.
- Essentially a declarative statement for what operations are applied, organized into a computation graph

```scala
// This creates a Plan tree, no computation happens yet
val plan = Plan.MapOp(
  Plan.FilterOp(
    Plan.Source(partitions), 
    x => x > 2
  ), 
  x => x * 2
)
```

### 2. Task Layer

- Computation is broken down into actual execution units over a single piece of data (partition). One task always runs on exactly one partition.
- Computation is actually triggered with `task.run()`

```scala
val task = Task.MapTask(partition, x => x * 2)
val result = task.run() // Actually executes the computation
```

### 3. DistCollection Layer (User API)

- High-level API that users interact with

```scala
val dc = DistCollection(data, 2)
  .map(_ * 2)     // Creates Plan.MapOp
  .filter(_ > 4)  // Creates Plan.FilterOp
  .collect()      // Triggers execution via Tasks
```

### Example Walkthrough
User writes code with DistCollection API
```scala
// User code:
val result = DistCollection(Seq(1,2,3,4), 2)
  .map(_ * 2)
  .filter(_ > 4)
  .collect()
  ```

Step 1: Building Plan (Lazy)
```scala
// DistCollection.map() creates:
Plan.MapOp(Plan.Source([Partition([1,2]), Partition([3,4])]), x => x * 2)

// DistCollection.filter() creates:
Plan.FilterOp(Plan.MapOp(...), x => x > 4)
```

Step 2: Task Creation
```scala
// Executor.createTasks() converts the plan to tasks:
// For the FilterOp, it:
// 1. Recursively computes input partitions: [Partition([2,4]), Partition([6,8])]
// 2. Creates tasks: [FilterTask(Partition([2,4]), x > 4), FilterTask(Partition([6,8]), x > 4)]
```

Step 3: Task Execution
```scala
// Each task runs independently:
task1.run() // FilterTask on [2,4] -> [4]
task2.run() // FilterTask on [6,8] -> [6,8]
// Results are returned to `result` object
```