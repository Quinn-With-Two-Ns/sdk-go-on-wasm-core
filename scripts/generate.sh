#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
rust_dir="$repo_dir/internal/sdk-core"
wasm_input="$rust_dir/target/wasm32-wasip1/release-lto/temporalio_sdk_core_wasm_bridge.wasm"
wasm_optimized="$rust_dir/target/wasm32-wasip1/release-lto/temporalio_sdk_core_wasm_bridge.opt.wasm"
module_path="github.com/temporalio/sdk-go-on-wasm-core"
local_proto_dir="$rust_dir/crates/protos/protos/local"
api_proto_dir="$rust_dir/crates/protos/protos/api_upstream"

wasm2go_path=$(command -v wasm2go || true)
if [[ -z "$wasm2go_path" ]]; then
  echo "wasm2go is required; install it with:" >&2
  echo "  go install github.com/ncruces/wasm2go@latest" >&2
  exit 1
fi

mkdir -p "$repo_dir/internal/corewasm"

RUSTFLAGS='-C opt-level=z -C codegen-units=1 -C strip=symbols' \
  cargo +1.94 build \
  --manifest-path "$rust_dir/Cargo.toml" \
  --profile release-lto \
  --target wasm32-wasip1 \
  -p temporalio-sdk-core-wasm-bridge

wasm-opt -Oz \
  --enable-bulk-memory-opt \
  --enable-nontrapping-float-to-int \
  --strip-debug \
  --strip-producers \
  "$wasm_input" \
  -o "$wasm_optimized"

wasm2go_tmp_dir="$repo_dir/internal/corewasm/.wasm2go-check"
mkdir -p "$wasm2go_tmp_dir"
generated_tmp="$wasm2go_tmp_dir/generated.go"
cleanup_wasm2go_tmp() {
  rm -f -- "$generated_tmp"
  rmdir "$wasm2go_tmp_dir" 2>/dev/null || true
}
trap cleanup_wasm2go_tmp EXIT

"$wasm2go_path" -o "$generated_tmp" "$wasm_optimized"
if ! compile_output=$(go test "$generated_tmp" 2>&1); then
  echo "$compile_output" >&2
  exit 1
fi
mv "$generated_tmp" "$repo_dir/internal/corewasm/generated.go"

protoc \
  -I "$local_proto_dir" \
  -I "$api_proto_dir" \
  --go_out="$repo_dir" \
  --go_opt="module=$module_path" \
  --go_opt="Mtemporal/sdk/core/core_interface.proto=$module_path/internal/corepb" \
  --go_opt="Mtemporal/sdk/core/activity_result/activity_result.proto=$module_path/internal/corepb/activityresult" \
  --go_opt="Mtemporal/sdk/core/activity_task/activity_task.proto=$module_path/internal/corepb/activitytask" \
  --go_opt="Mtemporal/sdk/core/child_workflow/child_workflow.proto=$module_path/internal/corepb/childworkflow" \
  --go_opt="Mtemporal/sdk/core/common/common.proto=$module_path/internal/corepb/corecommon" \
  --go_opt="Mtemporal/sdk/core/external_data/external_data.proto=$module_path/internal/corepb/externaldata" \
  --go_opt="Mtemporal/sdk/core/nexus/nexus.proto=$module_path/internal/corepb/nexus" \
  --go_opt="Mtemporal/sdk/core/workflow_activation/workflow_activation.proto=$module_path/internal/corepb/workflowactivation" \
  --go_opt="Mtemporal/sdk/core/workflow_commands/workflow_commands.proto=$module_path/internal/corepb/workflowcommands" \
  --go_opt="Mtemporal/sdk/core/workflow_completion/workflow_completion.proto=$module_path/internal/corepb/workflowcompletion" \
  --go_opt="Mtemporal/sdk/core/wasm_bridge/wasm_bridge.proto=$module_path/internal/corepb/wasmbridge" \
  "$local_proto_dir/temporal/sdk/core/core_interface.proto" \
  "$local_proto_dir/temporal/sdk/core/activity_result/activity_result.proto" \
  "$local_proto_dir/temporal/sdk/core/activity_task/activity_task.proto" \
  "$local_proto_dir/temporal/sdk/core/child_workflow/child_workflow.proto" \
  "$local_proto_dir/temporal/sdk/core/common/common.proto" \
  "$local_proto_dir/temporal/sdk/core/external_data/external_data.proto" \
  "$local_proto_dir/temporal/sdk/core/nexus/nexus.proto" \
  "$local_proto_dir/temporal/sdk/core/workflow_activation/workflow_activation.proto" \
  "$local_proto_dir/temporal/sdk/core/workflow_commands/workflow_commands.proto" \
  "$local_proto_dir/temporal/sdk/core/workflow_completion/workflow_completion.proto" \
  "$local_proto_dir/temporal/sdk/core/wasm_bridge/wasm_bridge.proto" \
  2> >(sed -E '/: warning: Import .* is unused\.$/d' >&2)
