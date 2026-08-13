package invoker

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
)

func TestFunctionExecutesWithOptionalContext(t *testing.T) {
	registered, err := New(
		"activity",
		"greet",
		func(ctx context.Context, name string) (string, error) {
			if ctx == nil {
				t.Fatal("activity context is nil")
			}
			return "Hello, " + name, nil
		},
		reflect.TypeFor[context.Context](),
		false,
		"context.Context",
	)
	if err != nil {
		t.Fatal(err)
	}
	payloadConverter := converter.GetDefaultPayloadConverter()
	input, err := payloadConverter.ToPayloads("Temporal")
	if err != nil {
		t.Fatal(err)
	}
	result, err := registered.Execute(context.Background(), payloadConverter, input.Payloads)
	if err != nil {
		t.Fatal(err)
	}
	var decoded string
	if err := payloadConverter.FromPayload(result, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != "Hello, Temporal" {
		t.Fatalf("Execute result = %q", decoded)
	}
}

func TestFunctionRequiresConfiguredContext(t *testing.T) {
	_, err := New(
		"workflow",
		"test",
		func() error { return nil },
		reflect.TypeFor[*struct{}](),
		true,
		"*workflow.Context",
	)
	if err == nil || !strings.Contains(err.Error(), "must accept *workflow.Context first") {
		t.Fatalf("New error = %v", err)
	}
}
