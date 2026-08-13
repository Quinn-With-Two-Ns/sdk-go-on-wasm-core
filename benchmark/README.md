# Benchmark Harness

This module keeps the workflow throughput benchmark isolated from the root `go.mod` while still
testing the local checkout through:

```go
replace github.com/temporalio/sdk-go-on-wasm-core => ..
```

The workload starts five no-op activities before awaiting any result, making the five activity tasks
available to the worker in parallel. Both SDK workers reserve five eager activity slots per workflow
task, so all five same-queue activities can be returned directly with the workflow-task completion.
Start a local Temporal dev server, then run the benchmark sweep from this module:

```sh
TEMPORAL_BENCHMARK=1 go test -run '^$' \
  -bench '^BenchmarkFiveParallelActivitiesThroughput/(go-sdk|wasm)$' \
  -benchtime=1000x -count=3 -cpu=1,2,4,8
```

Run one 500-workflow round per SDK with:

```sh
TEMPORAL_BENCHMARK=1 go test -run '^$' \
  -bench '^BenchmarkFiveParallelActivitiesThroughput/(go-sdk|wasm)$' \
  -benchtime=500x -count=1 -cpu=8
```

That command measures 2,500 activities per SDK. The 20 excluded warm-up workflows execute another
100 activities per SDK before timing begins. The activity workload reports both `workflows/s` and
`activities/s`. It also fails unless every warm-up and measured activity was returned eagerly with
its workflow-task completion. The latest checked-in counterbalanced result is in
[ACTIVITY_RESULTS.md](ACTIVITY_RESULTS.md).

If the machine's full `GOMAXPROCS` value is greater than `8`, append it to `-cpu` so the sweep
includes `1/2/4/8/GOMAXPROCS`. Each sample:

- starts a fresh isolated worker child process per implementation
- excludes worker startup and 20 warm-up executions at measured concurrency from timing
- uses unique workflow IDs for every run
- keeps exactly `GOMAXPROCS` workflows in flight during measured work
- configures both workers with two workflow pollers and 1,000 workflow-task/cache slots
- verifies every workflow result before counting the operation

The activity workload also configures both workers with two activity pollers and 1,000 activity
execution slots, five eager reservations per workflow task, and verifies each of the five activity
results before completing its workflow.

Both workers use a 1,000-run workflow cache for this comparison. The explicit poller and capacity
settings keep worker configuration differences out of the comparison.

Set `TEMPORAL_ADDRESS` or `TEMPORAL_NAMESPACE` when the service does not use the local defaults.
Set `TEMPORAL_API_KEY` for Temporal Cloud; keep the key in the environment rather than commands,
logs, or checked-in files.

## Child-process profiles

Profiling is opt-in and should be run separately from the throughput numbers:

```sh
mkdir -p /tmp/temporal-benchmark-profiles
TEMPORAL_BENCHMARK=1 \
TEMPORAL_BENCHMARK_PROFILE_DIR=/tmp/temporal-benchmark-profiles \
go test -run '^$' -bench '^BenchmarkFiveParallelActivitiesThroughput/(go-sdk|wasm)$' \
  -benchtime=1000x -count=1 -cpu=8
```

When `TEMPORAL_BENCHMARK_PROFILE_DIR` is set, each worker child writes CPU, heap, mutex, and block
profiles plus a tab-separated phase-timing summary after the parent sends a finalize request over
inherited pipes. The timing summary includes count, total, average, p50, p95, p99, and maximum for
Go SDK request metrics and WASM callback/bridge phases. The parent waits for the acknowledgement
before terminating the child so every diagnostic file is closed cleanly.

Inspect the captured profiles with `go tool pprof`, for example:

```sh
go tool pprof -top /tmp/temporal-benchmark-profiles/go-sdk-...-cpu.pprof
go tool pprof -top /tmp/temporal-benchmark-profiles/wasm-...-heap.pprof
go tool pprof -top /tmp/temporal-benchmark-profiles/wasm-...-mutex.pprof
go tool pprof -top /tmp/temporal-benchmark-profiles/wasm-...-block.pprof
```
