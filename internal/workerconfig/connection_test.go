package workerconfig

import (
	"testing"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
)

func TestNormalizeAppliesSharedDefaults(t *testing.T) {
	connection, err := Normalize("workflow", Connection{TaskQueue: "queue"})
	if err != nil {
		t.Fatal(err)
	}
	if connection.Address != "localhost:7233" || connection.Namespace != "default" || connection.Identity != "go-sdk-on-wasm-core" {
		t.Fatalf("Normalize defaults = %q %q %q", connection.Address, connection.Namespace, connection.Identity)
	}
	if connection.PayloadConverter == nil {
		t.Fatal("Normalize did not install the default payload converter")
	}
	if connection.PayloadCodec == nil {
		t.Fatal("Normalize did not install the default payload codec")
	}
}

func TestNormalizePreservesExplicitValues(t *testing.T) {
	payloadConverter := converter.GetDefaultPayloadConverter()
	payloadCodec := converter.NewNoopPayloadCodec()
	want := Connection{
		Address:          "temporal.example:7233",
		Namespace:        "payments",
		TaskQueue:        "payments-worker",
		Identity:         "worker-1",
		PayloadConverter: payloadConverter,
		PayloadCodec:     payloadCodec,
		APIKey:           "secret",
	}
	got, err := Normalize("activity", want)
	if err != nil {
		t.Fatal(err)
	}
	if got.Address != want.Address || got.Namespace != want.Namespace || got.TaskQueue != want.TaskQueue ||
		got.Identity != want.Identity || got.PayloadConverter != want.PayloadConverter ||
		got.PayloadCodec != want.PayloadCodec || got.APIKey != want.APIKey {
		t.Fatalf("Normalize changed explicit values: got %+v, want %+v", got, want)
	}
}

func TestNormalizeRequiresTaskQueue(t *testing.T) {
	_, err := Normalize("activity", Connection{})
	if err == nil || err.Error() != "activity task queue is required" {
		t.Fatalf("Normalize error = %v", err)
	}
}
