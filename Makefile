.PHONY: generate test check-public-packages demo

generate:
	./scripts/generate.sh

test: generate
	cargo +1.94 test --manifest-path internal/sdk-core/Cargo.toml -p temporalio-sdk-core-wasm-bridge
	./scripts/check-public-packages.sh
	go test ./...

check-public-packages:
	./scripts/check-public-packages.sh

demo: generate
	go run ./cmd/demo
