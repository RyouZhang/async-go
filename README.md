# async-go

A lightweight Go library for asynchronous programming patterns, including concurrent execution, batching, request merging, KV caching, and worker pool management.

## Installation

```bash
go get github.com/RyouZhang/async-go
```

## Core Functions (`async.go`)

### Panic Recovery

```go
// Set a global panic handler
async.SetPanicHandler(func(err any) {
    log.Println("recovered:", err)
})

// Run a function with panic recovery
res, err := async.Safety(func() (any, error) {
    return doSomething()
})
```

### Lambda / Call

Execute a function asynchronously with an optional timeout. Pass `async.Unlimit` (0) for no timeout.

```go
// Lambda: run a closure with timeout
res, err := async.Lambda(func() (any, error) {
    return fetchData()
}, 5*time.Second)

// Call: run a Method with arguments and timeout
res, err := async.Call(myMethod, 5*time.Second, arg1, arg2)
```

### Retry

Retry a function up to `maxCount` times with incremental backoff.

```go
res, err := async.Retry(func() (any, error) {
    return unreliableCall()
}, 3, 500*time.Millisecond)
```

### Concurrent Execution

```go
methods := []async.LambdaMethod{taskA, taskB, taskC}

// All: run all concurrently, wait for all to complete
results := async.All(methods, 10*time.Second)

// Serise: run sequentially, stop on first error
results := async.Serise(methods, 10*time.Second)

// Any: run all concurrently, fail fast on first error
results, err := async.Any(methods, 10*time.Second)

// AnyOne: run all concurrently, succeed fast on first success
res, errs := async.AnyOne(methods, 10*time.Second)
```

### Flow

Chain a series of methods where each step receives the output of the previous one.

```go
res, err := async.Flow(enterMethod, []any{initialArg}, []async.Method{step1, step2}, 5*time.Second)
```

### Parallel / Foreach / For

Execute tasks concurrently with a concurrency limit.

```go
// Parallel: run lambdas with max concurrency
results := async.Parallel(methods, 8)

// Foreach: iterate over a slice concurrently
results := async.Foreach(items, func(i int) (any, error) {
    return process(items[i])
}, 16)

// For: run a method for indices [0, count) concurrently
results := async.For(100, func(i int) (any, error) {
    return compute(i)
}, 32)
```

## Worker Pool (`pool.go`)

Register named worker pools with a fixed concurrency limit, then execute tasks against them.

```go
// Register a pool with max 10 concurrent workers
async.RegisterPool("my-pool", 10)

// Execute 100 tasks using the pool
results := async.Pool("my-pool", 100, func(i int) (any, error) {
    return process(i)
})
```

## Task Group (`task/` sub-package)

> **Replaces the deprecated `batch.go`** — use `task` for all new batch/grouping workloads.

The `task` sub-package provides a powerful task scheduling system with support for **batching**, **merging** (deduplication), and **grouping**.

### Interfaces

| Interface | Method | Description |
|-----------|--------|-------------|
| `Task` | `UniqueId() string` | **Required.** Must return a unique identifier for the task. |
| `MergeTask` | `MergeBy() string` | Optional. Tasks with the same `MergeBy` key share a single result. |
| `GroupTask` | `GroupBy() string` | Optional. Tasks with the same `GroupBy` key are batched together. |

### Usage

```go
import "github.com/RyouZhang/async-go/task"

// Register a task group with a batch handler and options
task.RegisterTaskGroup("fetch-users", func(tasks ...task.Task) (map[string]any, error) {
    // Process tasks in batch, return results keyed by UniqueId
    results := make(map[string]any)
    for _, t := range tasks {
        results[t.UniqueId()] = fetchUser(t)
    }
    return results, nil
}, task.DefaultOption().WithBatchSize(64).WithMaxWoker(4).WithTimeRange(20))

// Execute a single task
res, err := task.Exec(ctx, "fetch-users", myTask)

// Execute multiple tasks
results := task.BatchExec(ctx, "fetch-users", task1, task2, task3)
```

### Options

```go
opt := task.DefaultOption()       // batchSize=32, maxWorker=8, timeRange=10ms
opt.WithBatchSize(64)             // Max tasks per batch
opt.WithMaxWoker(16)              // Max concurrent workers
opt.WithTimeRange(50)             // Timer interval in milliseconds for flushing pending tasks
```

### Update at Runtime

```go
task.UpdateTaskGroup("fetch-users", task.DefaultOption().WithMaxWoker(16))
```

## Request Merging (`merge.go`)

Deduplicate concurrent requests by key — only one execution per unique key, with the result shared across all callers.

```go
m := async.NewMerge()
defer m.Destory()

// Concurrent calls with the same key will share one execution
res, err := m.Exec("user:123", func() (any, error) {
    return fetchFromDB("user:123")
})
```

## KV Cache (`kvcache.go`)

A goroutine-safe in-memory key-value cache with optional TTL, LRU eviction, and max size limit. All operations are serialized through an internal run loop.

```go
cache := async.NewKVCache().TTL(300).LRU(true).MaxSize(1000)
defer cache.Destory()

// Write
cache.Commit(func(kv *async.KVData) (any, error) {
    kv.Set("key1", "value1")
    kv.MSet([]any{"k2", "k3"}, []any{"v2", "v3"})
    return nil, nil
})

// Read
res, err := cache.Commit(func(kv *async.KVData) (any, error) {
    return kv.Get("key1")
})

// Delete
cache.Commit(func(kv *async.KVData) (any, error) {
    kv.Del("key1")
    return nil, nil
})
```

## ~~Batch (`batch.go`)~~ — DEPRECATED

> **`batch.go` is deprecated.** Use the [`task`](#task-group-task-sub-package) sub-package instead, which provides a more flexible and feature-rich replacement with support for task merging, grouping, configurable worker pools, and runtime option updates.

<details>
<summary>Legacy API reference (click to expand)</summary>

`batch.go` provided a global batch request system using `RegisterGroup`, `Get`, `ForceGet`, `MGet`, and `ForceMGet`. It relied on global state with a single run loop and lacked support for task grouping, merging, and worker concurrency control.

```go
// DEPRECATED — migrate to task sub-package
async.RegisterGroup("users", 50, batchFetchUsers, cacheProvider)
res, err := async.Get("users", userId)
results := async.MGet("users", id1, id2, id3)
```

**Migration guide:**
1. Define a struct implementing `task.Task` (and optionally `task.MergeTask` / `task.GroupTask`).
2. Replace `RegisterGroup` with `task.RegisterTaskGroup`.
3. Replace `Get` / `MGet` with `task.Exec` / `task.BatchExec`.
4. Handle caching externally or within your batch handler.

</details>

## License

[MIT](LICENSE) — Copyright (c) 2017 Ryou Zhang
