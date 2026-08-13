package activityworker

import (
	"context"
	"reflect"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/invoker"
	commonpb "go.temporal.io/api/common/v1"
)

type registeredActivity struct {
	name     string
	function *invoker.Function
}

func newRegisteredActivity(name string, function any) (*registeredActivity, error) {
	registered, err := invoker.New(
		"activity", name, function,
		reflect.TypeFor[context.Context](), false, "context.Context",
	)
	if err != nil {
		return nil, err
	}
	return &registeredActivity{name: name, function: registered}, nil
}

func (a *registeredActivity) execute(
	ctx context.Context,
	payloadConverter converter.PayloadConverter,
	input []*commonpb.Payload,
) (*commonpb.Payload, error) {
	return a.function.Execute(ctx, payloadConverter, input)
}
