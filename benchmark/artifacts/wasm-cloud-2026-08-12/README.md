# WASM-only Temporal Cloud profile

This capture profiles only the WASM-backed workflow worker. It ran 20 warm-up workflows followed by
500 measured no-op workflows at `GOMAXPROCS=8` against Temporal Cloud. The measured throughput was
5.402 workflows/s.

## Flamegraphs

- `wasm-cpu-flamegraph.png` renders CPU samples from the full 500-workflow child process.
- `wasm-sdk-focused-cpu-flamegraph.png` retains only samples whose stack passes through this SDK's
  packages and rescales them to 100%.
- `wasm-block-flamegraph.png` renders aggregate goroutine blocking delay from the same process.

The block graph is necessary for interpretation: the process used only 1.38 seconds of CPU during a
97.51-second CPU profile, so a CPU graph alone cannot reveal the elapsed-time bottleneck.

## Findings

1. **Callback gRPC response wait is the dominant bottleneck.** `RawGRPCHost.CallContext` through
   `grpc.ClientConn.Invoke` and `ClientStream.RecvMsg` accumulated 224.35 seconds of concurrent
   blocking delay. The leaf wait is `ClientStream.waitOnHeader`, so time is spent awaiting remote
   response headers rather than encoding or executing workflow code.
2. **Workflow completion is the actionable half of that wait.** `CompleteWorkflow` accumulated
   99.75 seconds across 500 completions, about 199.5 ms per workflow before concurrency overlap.
   `PollWorkflow` accumulated 96.53 seconds, but a continuously outstanding long poll is expected
   worker behavior and is not itself a throughput defect. Source attribution puts 99.20 of the
   completion path's 99.75 seconds specifically in `waitForWake`; taking the completed result used
   only 0.52 seconds.
3. **The worker is not CPU-bound.** Total CPU samples were 1.38 seconds (1.42% of profile duration).
   Runtime wake/sleep scheduling was 59.4% of sampled CPU. Core task draining was 13.0% cumulative,
   and taking workflow completions was 8.7% cumulative. Their absolute costs are too small to
   explain the wall-time regression. Only 260 ms of CPU samples passed through SDK-owned stacks,
   approximately 0.52 ms per measured workflow.
4. **Locks are not a bottleneck.** The mutex profile recorded only 1.23 ms of aggregate delay.
5. **Allocation is secondary.** The heap profile sampled 31.0 MiB allocated and 6.0 MiB retained.
   WASM memory growth was the largest entry: 4.47 MiB allocated and 2.47 MiB retained. Bridge
   `ownerCall` allocated about 2.0 MiB, all attributed to the per-call buffered reply channel at
   `corebridge/bridge.go:473`. These are optimization candidates, but neither correlates with the
   measured elapsed-time ceiling.
6. **Per-run serialization is small for this workload.** `workflowRunTurn.wait` accumulated 8.37
   seconds, 1.07% of total blocking mass, across unique workflow runs.

Blocking profiles sum delay across concurrent goroutines; their totals can exceed wall time and
must not be read as sequential elapsed time. The completion average is useful because this workload
submits exactly one completion per workflow, but it still includes overlap among eight workflows.

## Bottleneck priority

1. Instrument callback RPC latency by RPC name and phase (`PollWorkflowTaskQueue` versus
   `RespondWorkflowTaskCompleted`) on both sides of the WASM boundary.
2. Reduce or bypass bridge round trips needed to start, wake, drain, and take a workflow completion;
   verify improvement with the same Cloud capture.
3. Only then consider reducing runtime wakeups, `ownerCall` allocations, and WASM memory growth.

## Full profile set

The full measured-run prefix is:

```text
wasm-workflow-benchmark-wasm-1786583755318468000-queue-1786583755336205000
```

The directory also contains the short calibration profile produced by Go's benchmark harness. The
flamegraphs and text reports use the larger measured-run profile above.
