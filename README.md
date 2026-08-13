# Temporal Go workflow and activity workers on WASM Core

This library embeds the real Temporal Core worker by compiling the `internal/sdk-core` submodule to WASM,
translating it to Go with `wasm2go`, and using Go gRPC for network transport. Core owns workflow
and activity polling, workflow history/replay, activity retries, heartbeats, cancellation,
completion, and shutdown.

## Try it

Start Temporal:

```
temporal server start-dev --headless --dynamic-config-value activity.enableStandalone=true
```

Start the Core-backed worker:

```
make demo
```

To connect the demo to Temporal Cloud with an API key, set the Cloud endpoint and namespace. Supplying
the API key enables TLS automatically:

```
TEMPORAL_ADDRESS=<namespace>.<account>.tmprl.cloud:7233 \
TEMPORAL_NAMESPACE=<namespace>.<account> \
TEMPORAL_API_KEY=<api-key> \
make demo
```

Run the timer workflow, which waits five seconds, schedules `greet`, and returns its typed result:

```
temporal workflow execute --address localhost:7233 --namespace default --workflow-id go-wasm-demo --type timer-greeting --task-queue go-wasm-demo --input '"Temporal"' --tls=false
```

The result is:

```
"Hello, Temporal from Temporal Core compiled through WASM into Go!"
```

To run the child-workflow example on the same worker:

```
temporal workflow execute --address localhost:7233 --namespace default --workflow-id go-wasm-child-demo --type child-workflow-greeting --task-queue go-wasm-demo --input '"Temporal"' --tls=false
```

The parent starts `childGreeting`, waits for both its start acknowledgement and completion, and
returns `"Hello, Temporal from a child workflow!"`.

Workflow calls use function references and typed futures by default:

```go
greeting, err := workflow.ExecuteActivity[string](ctx, greet, workflow.ActivityOptions{
	StartToCloseTimeout: 10 * time.Second,
}, name).Get(ctx)
```

Register function-based operations through `RegisterActivity` and `RegisterWorkflow`; registration
and execution derive the same Temporal type name from the Go function. Use the `Register*Untyped`
variants when an explicit type name is required.

Dynamic workflows can call cross-language or otherwise runtime-selected operation names through
`ExecuteActivityUntyped` and `ExecuteChildWorkflowUntyped`; those futures retain pointer-based result
decoding.

The parallel example starts two deterministic workflow goroutines. Each schedules an activity and a
child workflow before awaiting either future, so all four operations are issued from the same
workflow task:

```
temporal workflow execute --address localhost:7233 --namespace default --workflow-id go-wasm-parallel-demo --type parallel-greeting --task-queue go-wasm-demo --input '"Temporal"' --tls=false
```

## Build

```
git submodule update --init
make generate
go test ./...
```

`make generate` builds the WASIp1 bridge, runs `wasm-opt`, translates it with `wasm2go`, and
generates the Core protobuf bindings. The generated approximately 25 MB
`internal/corewasm/generated.go` is ignored.

The Go host and Rust WASM gRPC transport share
`internal/sdk-core/crates/protos/protos/local/temporal/sdk/core/wasm_bridge/wasm_bridge.proto` as their wire
contract. `make generate` produces both language bindings from that file. The messages deliberately
have no protocol-version field because the Go host and embedded WASM module are generated and
shipped together; Protobuf field numbers provide the schema-evolution boundary.

The supported library surface is intentionally limited to `activity`, `workflow`, and `worker`,
with runnable programs under `cmd`. Workflow code imports `workflow`, activity code imports
`activity`, and registration plus lifecycle live in `worker`. Core bridging, payload conversion,
and worker execution machinery are implementation details under `internal` and carry no public
compatibility contract. `worker.New` creates the single public worker abstraction, which owns both
the workflow and activity workers for its task queue. This enables same-queue eager activity dispatch;
it reserves up to three eager activity slots per workflow task by default, configurable through
`MaxEagerActivityReservationsPerWorkflowTask`.

Payload handling follows Core SDK layering. `worker.Options.PayloadConverter` only converts between Go
values and Temporal payloads. `worker.Options.PayloadCodec` separately transforms complete Core
messages after polling and before completions or heartbeats are submitted, so compression or
encryption is applied exactly once across nested payload fields. Search attributes remain
server-readable and are not passed through the codec.
See [MIGRATING.md](MIGRATING.md) for the import and call-site changes from the prototype layout.

Core manages concurrent activity slots and server pollers. By default the activity worker allows 1,000
activity executions and lets Core select fixed or autoscaling pollers from server capabilities;
both limits are configurable in `worker.Options`.

The workflow worker likewise defaults to 1,000 workflow-task execution slots, lets Core select its
pollers, and caches up to 32 workflow runs. These capacities are configurable through
`worker.Options`. The language side still issues exactly one workflow poll at a time, while
processing different runs and submitting their completions concurrently.

The workflow interpreter supports typed workflow inputs/results, durable timers, asynchronous remote
activities and child workflows, deterministic workflow goroutines, concurrent commands, replay after
restart, and `RemoveFromCache`. Its dispatcher uses Go's `iter.Pull` coroutine primitive to run one
workflow coroutine at a time in stable creation order. Signals, queries, cancellation, local
activities, workflow API sandboxing, and production deadlock/determinism checks are not implemented.

`worker.Options` accepts `TLS` for an explicit `tls.Config` and `APIKey` for static bearer
authentication. Supplying an API key without an explicit TLS configuration enables TLS with the
system certificate pool, which is sufficient for Temporal Cloud. Dynamic API-key rotation, mTLS
convenience helpers, and the remaining production transport hardening are not implemented yet.
The bridge grows its reusable result scratch buffer on demand, up to a 64 MiB per-result safety
limit, instead of retaining a fixed 16 MiB buffer for every bridge instance.
Cancellation and shutdown detach handlers that ignore their cancelled context; Go cannot forcibly
stop those goroutines, so activity code must still honor context cancellation to stop its own work.

## Benchmark

The benchmark compares the official Go SDK worker and the WASM-backed worker against a Temporal
server. Every workflow starts five no-op activities in parallel, then waits for and validates all
five results. The benchmark measures sustained end-to-end throughput, including client RPCs, server
scheduling, polling, workflow and activity task processing, and completion. Worker startup and a
warm-up phase per implementation are excluded. Both implementations reserve five eager activity
slots per workflow task so the comparison exercises same-worker eager dispatch for all five tasks.

Start Temporal as shown above, then run:

```
TEMPORAL_BENCHMARK=1 go -C benchmark test -run '^$' -bench . \
  -benchtime=1000x -count=3 -cpu=1,2,4,8
```

Run one 500-workflow round of the five-parallel-activity workload per SDK with:

```
TEMPORAL_BENCHMARK=1 go -C benchmark test -run '^$' \
  -bench '^BenchmarkFiveParallelActivitiesThroughput/(go-sdk|wasm)$' \
  -benchtime=500x -count=1 -cpu=8
```

Set `TEMPORAL_ADDRESS` or `TEMPORAL_NAMESPACE` when the server does not use the local defaults. Set
`TEMPORAL_API_KEY` when connecting to Temporal Cloud; it authenticates the benchmark client and both
worker implementations and enables TLS. For example:

```
TEMPORAL_BENCHMARK=1 \
TEMPORAL_ADDRESS=<namespace>.<account>.tmprl.cloud:7233 \
TEMPORAL_NAMESPACE=<namespace>.<account> \
TEMPORAL_API_KEY=<api-key> \
go -C benchmark test -run '^$' -bench . -benchtime=1000x
```

The benchmark is opt-in so `go test ./...` never requires a running server. Each implementation runs
alone in an isolated child process, and process startup is excluded from the timed results. Each
sample also excludes 20 warm-up executions, uses globally unique workflow IDs, and keeps
`GOMAXPROCS` workflows in flight. It reports time per completed workflow, `workflows/s`, and
`activities/s`. The first command above processes 1,000 workflows and 5,000 activities per
implementation in each of three repetitions. Run against an otherwise idle local server for the most
repeatable results. Because Temporal server scheduling is part of every measurement, these numbers
compare end-to-end throughput rather than isolated worker CPU time.
