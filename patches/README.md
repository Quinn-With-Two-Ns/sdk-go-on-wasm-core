# Core patches

`internal/sdk-core` is a submodule pinned to an upstream `temporalio/sdk-rust` commit, so changes to
the Rust bridge and to Core itself cannot be committed to this repository. This directory keeps
those changes as patches so `internal/corewasm/generated.go` stays reproducible.

`internal/corewasm/generated.go` is built from the pinned submodule **with every patch here
applied**. Running `make generate` against a clean submodule regenerates a module without them, and
silently drops the behavior the patch adds.

Apply them before regenerating:

```
git submodule update --init
git -C internal/sdk-core apply ../../patches/*.patch
make generate
```

Each patch should be upstreamed to `temporalio/sdk-rust` and removed from this directory once the
submodule pin moves past it.

## sdk-core-worker-status.patch

Lets the worker report its status to the server, which is what `temporal worker list` and
`temporal worker describe` read. Four changes:

- The bridge's `temporal_core_init_with_worker_options` ABI takes a heartbeat interval and passes it
  to `RuntimeOptions::heartbeat_interval`, which the bridge previously hardcoded to `None`.
- Worker construction runs inside the Tokio runtime, not only inside the local set. Core spawns its
  shared per-namespace worker (the one that sends the status reports) during construction, and
  `tokio::spawn` panics without a runtime in context.
- `WorkerHostInfo.process_id` is left empty on WASM targets. `std::process::id` panics under WASIp1.
- Worker status uses a no-op system resource source on WASM targets. `RealSysInfo` refreshes on a
  background thread, and WASIp1 cannot spawn threads. WASM cannot observe its host's CPU or memory
  either way, so the reported usage is zero.

It also sets the connection's `client_name`/`client_version` so status reports carry the Go SDK
identity instead of Core's own `temporal-rust` default, matching the metadata `internal/wasmhost`
already puts on every request.
