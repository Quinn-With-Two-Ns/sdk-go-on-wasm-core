package worker_test

import (
	"context"
	"testing"

	"github.com/temporalio/sdk-go-on-wasm-core/activity"
	"github.com/temporalio/sdk-go-on-wasm-core/worker"
	"github.com/temporalio/sdk-go-on-wasm-core/workflow"
	commonpb "go.temporal.io/api/common/v1"
)

type testPayloadConverter struct{}

type testPayloadCodec struct{}

func (*testPayloadConverter) ToPayload(any) (*commonpb.Payload, error)      { return nil, nil }
func (*testPayloadConverter) FromPayload(*commonpb.Payload, any) error      { return nil }
func (*testPayloadConverter) ToPayloads(...any) (*commonpb.Payloads, error) { return nil, nil }
func (*testPayloadConverter) FromPayloads(*commonpb.Payloads, ...any) error { return nil }

func (*testPayloadCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return payloads, nil
}
func (*testPayloadCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return payloads, nil
}

func TestPublicPackagesComposeWithoutImplementationImports(t *testing.T) {
	var payloadConverter worker.PayloadConverter = &testPayloadConverter{}
	var payloadCodec worker.PayloadCodec = &testPayloadCodec{}
	options := worker.Options{TaskQueue: "test", PayloadConverter: payloadConverter, PayloadCodec: payloadCodec}
	if options.PayloadConverter != payloadConverter || options.PayloadCodec != payloadCodec {
		t.Fatal("worker options did not retain converter components")
	}

	var heartbeat func(context.Context, ...any) error = activity.RecordHeartbeat
	var newWorker func(worker.Options) (*worker.Worker, error) = worker.New
	var newFuture func(*workflow.Context) (workflow.Future[string], workflow.Settable[string]) = workflow.NewFuture[string]
	var newFutureUntyped func(*workflow.Context) (workflow.UntypedFuture, workflow.UntypedSettable) = workflow.NewFutureUntyped
	var executeActivity func(*workflow.Context, any, workflow.ActivityOptions, ...any) workflow.Future[string] = workflow.ExecuteActivity[string]
	var executeActivityUntyped func(*workflow.Context, string, workflow.ActivityOptions, ...any) workflow.UntypedFuture = workflow.ExecuteActivityUntyped
	var executeLocalActivity func(*workflow.Context, any, workflow.LocalActivityOptions, ...any) workflow.Future[string] = workflow.ExecuteLocalActivity[string]
	var executeLocalActivityUntyped func(*workflow.Context, string, workflow.LocalActivityOptions, ...any) workflow.UntypedFuture = workflow.ExecuteLocalActivityUntyped
	var executeChildWorkflow func(*workflow.Context, any, workflow.ChildWorkflowOptions, ...any) workflow.ChildWorkflowFuture[string] = workflow.ExecuteChildWorkflow[string]
	var executeChildWorkflowUntyped func(*workflow.Context, string, workflow.ChildWorkflowOptions, ...any) workflow.UntypedChildWorkflowFuture = workflow.ExecuteChildWorkflowUntyped
	var getSignalChannel func(*workflow.Context, string) workflow.ReceiveChannel[string] = workflow.GetSignalChannel[string]
	var getSignalChannelUntyped func(*workflow.Context, string) workflow.UntypedReceiveChannel = workflow.GetSignalChannelUntyped
	var setQueryHandler func(*workflow.Context, string, any) error = workflow.SetQueryHandler
	var setUpdateHandler func(*workflow.Context, string, any) error = workflow.SetUpdateHandler
	var setUpdateHandlerWithOptions func(*workflow.Context, string, any, workflow.UpdateHandlerOptions) error = workflow.SetUpdateHandlerWithOptions
	var getCurrentUpdateInfo func(*workflow.Context) *workflow.UpdateInfo = workflow.GetCurrentUpdateInfo
	var registerWorkflow func(*worker.Worker, any) = (*worker.Worker).RegisterWorkflow
	var registerWorkflowUntyped func(*worker.Worker, string, any) = (*worker.Worker).RegisterWorkflowUntyped
	var registerActivity func(*worker.Worker, any) = (*worker.Worker).RegisterActivity
	var registerActivityUntyped func(*worker.Worker, string, any) = (*worker.Worker).RegisterActivityUntyped
	_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _ = heartbeat, newWorker, newFuture, newFutureUntyped,
		executeActivity, executeActivityUntyped, executeLocalActivity, executeLocalActivityUntyped, executeChildWorkflow, executeChildWorkflowUntyped,
		getSignalChannel, getSignalChannelUntyped, setQueryHandler, setUpdateHandler, setUpdateHandlerWithOptions, getCurrentUpdateInfo,
		registerWorkflow, registerWorkflowUntyped, registerActivity, registerActivityUntyped
}
