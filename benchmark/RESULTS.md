# Temporal Cloud Workflow Throughput Results

> Historical result: this no-op workflow workload is no longer part of the benchmark harness. The
> current five-parallel-activity result is in [ACTIVITY_RESULTS.md](ACTIVITY_RESULTS.md).

Date: 2026-08-12

## Conclusion

The WASM-backed worker now meets and exceeds Temporal Go SDK v1.47.0 throughput for the supported
no-op workflow path. After fixing post-response bridge progression and matching the Go SDK's DNS
round-robin transport policy, WASM was faster in all five alternating 500-workflow Temporal Cloud
pairs at `GOMAXPROCS=8`. The median paired WASM/Go ratio was 105.4%, a 5.4% lead (one-sided sign test
p=0.031).

Before these fixes, the same methodology measured WASM at a median 86.9% of Go throughput, a 13.1%
regression with WASM slower in all five pairs. The combined changes therefore moved the median
paired ratio by 18.5 percentage points. A follow-up three-pair test with only round-robin removed
dropped WASM back to a median 87.9% of Go throughput, identifying connection selection as the
dominant measured latency source. The bridge recheck remains because it closes a verified missed
progression edge; its independent throughput contribution was not isolated.

These are end-to-end Temporal Cloud results. They include network latency, server scheduling,
polling, workflow-task processing, and completion. They do not isolate workflow interpreter CPU
cost.

## Environment

- Apple M4 Max, Darwin arm64, 14 logical CPUs
- Go 1.26.5
- Temporal Cloud endpoint supplied through `TEMPORAL_ADDRESS`
- Temporal Go SDK v1.47.0
- 20 warm-up workflows at measured concurrency
- two workflow pollers and 1,000 workflow-task/cache slots for both workers
- 8 workflows in flight
- 500 measured workflows per implementation and pair
- alternating execution order to limit time/order bias

The API key was supplied through `TEMPORAL_API_KEY` and is intentionally absent from commands and
results.

## Sustained Results After Latency Fixes

| Pair | Order | Go SDK workflows/s | WASM workflows/s | WASM / Go | WASM lead |
| ---: | :---: | ---: | ---: | ---: | ---: |
| 1 | Go, WASM | 6.604 | 7.046 | 106.7% | 6.7% |
| 2 | WASM, Go | 6.485 | 6.835 | 105.4% | 5.4% |
| 3 | Go, WASM | 6.562 | 7.317 | 111.5% | 11.5% |
| 4 | WASM, Go | 6.772 | 7.099 | 104.8% | 4.8% |
| 5 | Go, WASM | 6.590 | 6.662 | 101.1% | 1.1% |
| **Median paired ratio** | | | | **105.4%** | **5.4%** |

The raw output is retained outside the repository in
`/tmp/sdk-go-on-wasm-cloud-after-recheck-roundrobin-500x.txt`.

## Controlled Transport Attribution

The bridge recheck was kept enabled while only round-robin connection selection was temporarily
removed. WASM regressed in all three alternating 500-workflow pairs:

| Pair | Order | Go SDK workflows/s | WASM workflows/s | WASM / Go | Regression |
| ---: | :---: | ---: | ---: | ---: | ---: |
| 1 | Go, WASM | 6.868 | 5.081 | 74.0% | 26.0% |
| 2 | WASM, Go | 6.816 | 5.990 | 87.9% | 12.1% |
| 3 | Go, WASM | 6.805 | 5.999 | 88.2% | 11.8% |
| **Median paired ratio** | | | | **87.9%** | **12.1%** |

Round-robin was restored after the diagnostic. The raw attribution output is retained in
`/tmp/sdk-go-on-wasm-cloud-attribution-recheck-only-500x.txt`.

## Baseline Before Latency Fixes

| Pair | Order | Go SDK workflows/s | WASM workflows/s | WASM / Go | Regression |
| ---: | :---: | ---: | ---: | ---: | ---: |
| 1 | Go, WASM | 6.473 | 5.627 | 86.9% | 13.1% |
| 2 | WASM, Go | 6.646 | 6.603 | 99.4% | 0.6% |
| 3 | Go, WASM | 6.666 | 5.454 | 81.8% | 18.2% |
| 4 | WASM, Go | 6.666 | 5.918 | 88.8% | 11.2% |
| 5 | Go, WASM | 6.252 | 4.999 | 80.0% | 20.0% |
| **Median** | | **6.646** | **5.627** | **86.9%** | **13.1%** |

The raw baseline output is retained outside the repository in:

- `/tmp/sdk-go-on-wasm-benchmark-cloud-final-500x-cpu8.txt`
- `/tmp/sdk-go-on-wasm-benchmark-cloud-final-500x-cpu8-tail.txt`

## Profile Findings

A separate 100-workflow profile run at `GOMAXPROCS=8` produced valid CPU, heap, mutex, and block
profiles for both worker child processes. It preceded the worker-configuration and parallel-warmup
corrections, so it is diagnostic evidence only and is not part of the reported throughput result.

A later WASM-only 500-workflow capture, including CPU and blocking flamegraphs, is retained under
`benchmark/artifacts/wasm-cloud-2026-08-12/`. Its findings supersede the shorter diagnostic profile
for WASM bottleneck attribution.

- Go SDK: 320 ms sampled CPU over 22.35 s (1.43%).
- WASM: 470 ms sampled CPU over 29.41 s (1.60%).
- WASM mutex contention totaled about 0.3 ms.
- WASM blocking time was dominated by callback gRPC waits: about 44.7 s across concurrent calls,
  with workflow completions accounting for about 22.3 s.
- The generated WASM module and bridge owner loop appeared in CPU profiles, but absolute CPU time
  was too small to explain the wall-time delta.

This baseline evidence ruled out CPU saturation and Go mutex contention as the primary regression
and localized the deficit to the Core-to-Go callback transport / remote RPC progression path. The
profile alone could not separate bridge scheduling from Temporal Cloud/server latency; the later
phase timing and controlled transport test supplied that attribution.

Two controlled experiments did not improve the first paired result and were reverted:

- restoring gzip on the Go callback transport
- reducing the bridge fallback timer from 50 ms to 1 ms

## Phase Timing After Bridge Progression Fix

The benchmark profiler now records per-phase latency distributions. A 100-workflow Cloud diagnostic
before enabling round-robin transport showed that local response propagation was not the remaining
bottleneck:

- callback response queue delay: p50 0.036 ms, p95 0.103 ms
- applying a response back into Core: p50 0.007 ms, p95 0.019 ms
- Go SDK `RespondWorkflowTaskCompleted`: p50 140.224 ms
- WASM callback `RespondWorkflowTaskCompleted`: p50 187.225 ms
- Go SDK `PollWorkflowTaskQueue`: p50 145.087 ms
- WASM callback `PollWorkflowTaskQueue`: p50 189.900 ms

The nearly identical 45-47 ms transport-side shift for both RPC types, combined with sub-millisecond
local response handling, moved the investigation from Go scheduling/copying to connection policy.
The Go SDK uses DNS round-robin by default while the callback host previously used grpc-go's default
selection policy. The later three-pair transport attribution confirmed the connection policy as the
dominant measured source.

The bridge progression defect was independently code-backed: after applying a response to Core,
`operationStep` returned pending without re-running guest tasks or checking whether the operation had
become ready. It now continues only when `pumpGRPC` actually applied a response, avoiding both the
unnecessary park and an idle spin loop.

## Harness Corrections

The benchmark was corrected before the sustained run:

- Both implementations now use two workflow pollers and 1,000 workflow-task/cache slots.
- Warm-up uses the same concurrency as measurement rather than warming serially.
- The parent/child profiling control pipe is wired in the correct direction; live Cloud profiling
  now finalizes all eight profile files successfully.

Run the comparison with credentials supplied only through the environment:

```sh
TEMPORAL_BENCHMARK=1 \
TEMPORAL_ADDRESS=<namespace>.<account>.tmprl.cloud:7233 \
TEMPORAL_NAMESPACE=<namespace>.<account> \
TEMPORAL_API_KEY=<api-key> \
go test -run '^$' -bench 'WorkflowThroughput/(go-sdk|wasm)$' \
  -benchtime=500x -count=5 -cpu=8
```

For an order-controlled run, invoke the anchored `go-sdk` and `wasm` sub-benchmarks separately and
alternate their order per pair.

## Scope and Remaining Risks

This result covers only the currently implemented single-task no-op workflow. It does not measure
timers, activities, replay/cache pressure, larger payloads, or unsupported Go SDK features. Temporal
Cloud variation remains visible even in 500-workflow samples, so future comparisons should keep
paired alternating trials and should not rely on a single benchmark process whose fixed subtest
order always runs Go first.
