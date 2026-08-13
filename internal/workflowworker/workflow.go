package workflowworker

import (
	"reflect"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/invoker"
	commonpb "go.temporal.io/api/common/v1"
)

type registeredWorkflow struct {
	name     string
	function *invoker.Function
}

func newRegisteredWorkflow(name string, function any) (*registeredWorkflow, error) {
	registered, err := invoker.New(
		"workflow", name, function,
		reflect.TypeFor[*Context](), true, "*workflow.Context",
	)
	if err != nil {
		return nil, err
	}
	return &registeredWorkflow{name: name, function: registered}, nil
}

func (w *registeredWorkflow) execute(
	ctx *Context,
	payloadConverter converter.PayloadConverter,
	input []*commonpb.Payload,
) (*commonpb.Payload, error) {
	return w.function.Execute(ctx, payloadConverter, input)
}
