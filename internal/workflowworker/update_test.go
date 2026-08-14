package workflowworker

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
	workflowactivation "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowactivation"
	workflowcommands "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcommands"
	workflowcompletion "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcompletion"
	commonpb "go.temporal.io/api/common/v1"
)

func TestUpdateAcceptsExecutesAndCompletes(t *testing.T) {
	worker := newTestWorker()
	worker.Register("updatable", func(ctx *Context) error {
		state := "waiting"
		if err := SetUpdateHandler(ctx, "set-state", func(_ *Context, next string) (string, error) {
			previous := state
			state = next
			return previous, nil
		}, UpdateHandlerOptions{}); err != nil {
			return err
		}
		if err := SetQueryHandler(ctx, "state", func() (string, error) { return state, nil }); err != nil {
			return err
		}
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "updatable", nil))

	completion := worker.handleActivation(activation(
		"run-1",
		updateJob(t, worker, "update-1", "protocol-1", "set-state", true, "ready"),
		queryJob(t, worker, "query-1", "state"),
	))
	responses := updateResponses(t, completion)
	if len(responses) != 2 || responses[0].GetAccepted() == nil || responses[1].GetCompleted() == nil {
		t.Fatalf("update responses = %v, want accepted then completed", responses)
	}
	if got := updateValue[string](t, worker, responses[1]); got != "waiting" {
		t.Fatalf("update result = %q, want waiting", got)
	}
	if got := queryValue[string](t, worker, completion); got != "ready" {
		t.Fatalf("query after update = %q, want ready", got)
	}
}

func TestUpdateHandlerCanBlockOnWorkflowTimer(t *testing.T) {
	worker := newTestWorker()
	worker.Register("updatable", func(ctx *Context) error {
		if err := SetUpdateHandler(ctx, "delayed", func(ctx *Context, value string) (string, error) {
			if err := Sleep(ctx, time.Second); err != nil {
				return "", err
			}
			return "done:" + value, nil
		}, UpdateHandlerOptions{}); err != nil {
			return err
		}
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "updatable", nil))

	started := successfulCommands(t, worker.handleActivation(activation(
		"run-1",
		updateJob(t, worker, "update-1", "protocol-1", "delayed", true, "work"),
	)))
	if len(started) != 2 || started[0].GetUpdateResponse().GetAccepted() == nil || started[1].GetStartTimer() == nil {
		t.Fatalf("start commands = %v, want accepted update and timer", started)
	}

	completed := updateResponses(t, worker.handleActivation(activation("run-1", fireTimerJob(1))))
	if len(completed) != 1 || completed[0].GetCompleted() == nil {
		t.Fatalf("completion responses = %v, want completed update", completed)
	}
	if got := updateValue[string](t, worker, completed[0]); got != "done:work" {
		t.Fatalf("delayed update result = %q", got)
	}
}

func TestUpdateValidatorRejectsBeforeAcceptance(t *testing.T) {
	worker := newTestWorker()
	handlerCalls := 0
	worker.Register("updatable", func(ctx *Context) error {
		if err := SetUpdateHandler(ctx, "positive", func(_ *Context, value int) error {
			handlerCalls++
			return nil
		}, UpdateHandlerOptions{Validator: func(value int) error {
			if value <= 0 {
				return errors.New("value must be positive")
			}
			return nil
		}}); err != nil {
			return err
		}
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "updatable", nil))

	responses := updateResponses(t, worker.handleActivation(activation(
		"run-1",
		updateJob(t, worker, "update-1", "protocol-1", "positive", true, 0),
	)))
	if len(responses) != 1 || responses[0].GetRejected() == nil {
		t.Fatalf("responses = %v, want one rejection", responses)
	}
	if message := failureMessage(responses[0].GetRejected()); !strings.Contains(message, "value must be positive") {
		t.Fatalf("rejection = %q", message)
	}
	if handlerCalls != 0 {
		t.Fatalf("handler calls = %d, want 0", handlerCalls)
	}
	if worker.getRun("run-1") == nil {
		t.Fatal("validator rejection evicted workflow")
	}
}

func TestUpdateReplaySkipsValidator(t *testing.T) {
	worker := newTestWorker()
	validatorCalls := 0
	handlerCalls := 0
	worker.Register("updatable", func(ctx *Context) error {
		if err := SetUpdateHandler(ctx, "increment", func(_ *Context, value int) (int, error) {
			handlerCalls++
			return value + 1, nil
		}, UpdateHandlerOptions{Validator: func(int) error {
			validatorCalls++
			return errors.New("validator must be skipped during replay")
		}}); err != nil {
			return err
		}
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})

	replay := initializeActivation("run-1", "updatable", nil)
	replay.IsReplaying = true
	replay.Jobs = append(replay.Jobs, updateJob(t, worker, "update-1", "protocol-1", "increment", false, 41))
	responses := updateResponses(t, worker.handleActivation(replay))
	if len(responses) != 2 || responses[0].GetAccepted() == nil || responses[1].GetCompleted() == nil {
		t.Fatalf("replay responses = %v", responses)
	}
	if validatorCalls != 0 || handlerCalls != 1 {
		t.Fatalf("validator calls = %d, handler calls = %d", validatorCalls, handlerCalls)
	}
	if got := updateValue[int](t, worker, responses[1]); got != 42 {
		t.Fatalf("replayed update result = %d", got)
	}
}

func TestUpdateSharesOneDecodedInputBetweenValidatorAndHandler(t *testing.T) {
	worker := newTestWorker()
	payloadConverter := &countingPayloadConverter{PayloadConverter: worker.payloadConverter}
	worker.payloadConverter = payloadConverter
	worker.Register("updatable", func(ctx *Context) error {
		if err := SetUpdateHandler(ctx, "double", func(_ *Context, value int) (int, error) {
			return value * 2, nil
		}, UpdateHandlerOptions{Validator: func(int) error { return nil }}); err != nil {
			return err
		}
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "updatable", nil))
	payloadConverter.fromPayloadsCalls = 0

	responses := updateResponses(t, worker.handleActivation(activation(
		"run-1",
		updateJob(t, worker, "update-1", "protocol-1", "double", true, 21),
	)))
	if payloadConverter.fromPayloadsCalls != 1 {
		t.Fatalf("payload decode calls = %d, want 1", payloadConverter.fromPayloadsCalls)
	}
	if got := updateValue[int](t, worker, responses[1]); got != 42 {
		t.Fatalf("update result = %d", got)
	}
}

func TestUpdateFailuresStayScopedToUpdate(t *testing.T) {
	worker := newTestWorker()
	worker.Register("updatable", func(ctx *Context) error {
		if err := SetUpdateHandler(ctx, "fail", func(*Context) error { return errors.New("handler failed") }, UpdateHandlerOptions{}); err != nil {
			return err
		}
		if err := SetUpdateHandler(ctx, "validate-panic", func(*Context) error { return nil }, UpdateHandlerOptions{
			Validator: func() error { panic("validator panic") },
		}); err != nil {
			return err
		}
		if err := SetQueryHandler(ctx, "health", func() (string, error) { return "healthy", nil }); err != nil {
			return err
		}
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "updatable", nil))

	tests := []struct {
		name       string
		updateName string
		want       string
		accepted   bool
	}{
		{name: "missing", updateName: "missing", want: "is not registered"},
		{name: "handler error", updateName: "fail", want: "handler failed", accepted: true},
		{name: "validator panic", updateName: "validate-panic", want: "validator panic"},
	}
	for i, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			responses := updateResponses(t, worker.handleActivation(activation(
				"run-1",
				updateJob(t, worker, fmt.Sprintf("update-%d", i), fmt.Sprintf("protocol-%d", i), testCase.updateName, true),
			)))
			wantCount := 1
			if testCase.accepted {
				wantCount = 2
				if responses[0].GetAccepted() == nil {
					t.Fatalf("first response = %v, want accepted", responses[0])
				}
			}
			if len(responses) != wantCount || responses[len(responses)-1].GetRejected() == nil {
				t.Fatalf("responses = %v, want rejection", responses)
			}
			if message := failureMessage(responses[len(responses)-1].GetRejected()); !strings.Contains(message, testCase.want) {
				t.Fatalf("rejection = %q, want %q", message, testCase.want)
			}
			if worker.getRun("run-1") == nil {
				t.Fatal("update failure evicted workflow")
			}
		})
	}

	if got := queryValue[string](t, worker, worker.handleActivation(activation("run-1", queryJob(t, worker, "query-1", "health")))); got != "healthy" {
		t.Fatalf("query after update failures = %q", got)
	}
}

func TestUpdateHandlerPanicFailsActivationForRetry(t *testing.T) {
	worker := newTestWorker()
	worker.Register("updatable", func(ctx *Context) error {
		if err := SetUpdateHandler(ctx, "panic", func(*Context) error {
			panic("handler panic")
		}, UpdateHandlerOptions{}); err != nil {
			return err
		}
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "updatable", nil))

	completion := worker.handleActivation(activation(
		"run-1",
		updateJob(t, worker, "update-1", "protocol-1", "panic", true),
	))
	failure := completion.GetFailed()
	if failure == nil || !strings.Contains(failureMessage(failure.Failure), "handler panic") {
		t.Fatalf("activation failure = %v, want handler panic", failure)
	}
	if worker.getRun("run-1") != nil {
		t.Fatal("workflow remained cached after update handler panic")
	}
}

func TestUpdateDecodeFailuresRespectAcceptanceBoundary(t *testing.T) {
	worker := newTestWorker()
	worker.Register("updatable", func(ctx *Context) error {
		if err := SetUpdateHandler(ctx, "validated", func(*Context, int) error { return nil }, UpdateHandlerOptions{
			Validator: func(int) error { return nil },
		}); err != nil {
			return err
		}
		if err := SetUpdateHandler(ctx, "unvalidated", func(*Context, int) error { return nil }, UpdateHandlerOptions{}); err != nil {
			return err
		}
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "updatable", nil))

	validated := updateResponses(t, worker.handleActivation(activation(
		"run-1",
		updateJob(t, worker, "update-1", "protocol-1", "validated", true, "not-an-int"),
	)))
	if len(validated) != 1 || validated[0].GetRejected() == nil {
		t.Fatalf("validated decode responses = %v", validated)
	}

	unvalidated := updateResponses(t, worker.handleActivation(activation(
		"run-1",
		updateJob(t, worker, "update-2", "protocol-2", "unvalidated", true, "not-an-int"),
	)))
	if len(unvalidated) != 1 || unvalidated[0].GetRejected() == nil {
		t.Fatalf("unvalidated decode responses = %v", unvalidated)
	}
}

func TestCurrentUpdateInfoIsAvailableAndPropagated(t *testing.T) {
	worker := newTestWorker()
	var validatorInfo *UpdateInfo
	var handlerInfo *UpdateInfo
	var childInfo *UpdateInfo
	var rootInfo *UpdateInfo
	worker.Register("updatable", func(ctx *Context) error {
		rootInfo = GetCurrentUpdateInfo(ctx)
		if err := SetUpdateHandler(ctx, "inspect", func(ctx *Context) (string, error) {
			handlerInfo = GetCurrentUpdateInfo(ctx)
			done, settable := NewFuture(ctx)
			Go(ctx, func(ctx *Context) {
				childInfo = GetCurrentUpdateInfo(ctx)
				settable.SetValue("done")
			})
			var value string
			return value, done.Get(ctx, &value)
		}, UpdateHandlerOptions{Validator: func(ctx *Context) error {
			validatorInfo = GetCurrentUpdateInfo(ctx)
			return nil
		}}); err != nil {
			return err
		}
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "updatable", nil))
	responses := updateResponses(t, worker.handleActivation(activation(
		"run-1",
		updateJob(t, worker, "update-id", "protocol-id", "inspect", true),
	)))
	if len(responses) != 2 || responses[1].GetCompleted() == nil {
		t.Fatalf("responses = %v", responses)
	}
	for name, info := range map[string]*UpdateInfo{
		"validator": validatorInfo,
		"handler":   handlerInfo,
		"child":     childInfo,
	} {
		if info == nil || info.ID != "update-id" || info.Name != "inspect" {
			t.Fatalf("%s update info = %+v", name, info)
		}
	}
	if rootInfo != nil {
		t.Fatalf("root update info = %+v, want nil", rootInfo)
	}
}

func TestMultipleUpdatesPreserveActivationOrder(t *testing.T) {
	worker := newTestWorker()
	worker.Register("updatable", func(ctx *Context) error {
		if err := SetUpdateHandler(ctx, "echo", func(_ *Context, value string) (string, error) { return value, nil }, UpdateHandlerOptions{}); err != nil {
			return err
		}
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "updatable", nil))

	responses := updateResponses(t, worker.handleActivation(activation(
		"run-1",
		updateJob(t, worker, "update-1", "protocol-1", "echo", true, "one"),
		updateJob(t, worker, "update-2", "protocol-2", "echo", true, "two"),
	)))
	if len(responses) != 4 {
		t.Fatalf("responses = %d, want 4", len(responses))
	}
	for i, protocolID := range []string{"protocol-1", "protocol-2"} {
		if responses[i].ProtocolInstanceId != protocolID || responses[i].GetAccepted() == nil {
			t.Fatalf("response %d = %v, want acceptance for %s", i, responses[i], protocolID)
		}
		completed := responses[i+2]
		if completed.ProtocolInstanceId != protocolID || completed.GetCompleted() == nil {
			t.Fatalf("response %d = %v, want completion for %s", i+2, completed, protocolID)
		}
	}
}

func TestUpdateHandlerRunsBeforeNormalWorkflowCoroutine(t *testing.T) {
	worker := newTestWorker()
	worker.Register("updatable", func(ctx *Context) error {
		order := make([]string, 0, 2)
		if err := SetUpdateHandler(ctx, "record", func(*Context) error {
			order = append(order, "update")
			return nil
		}, UpdateHandlerOptions{}); err != nil {
			return err
		}
		if err := SetQueryHandler(ctx, "order", func() ([]string, error) { return order, nil }); err != nil {
			return err
		}
		if err := GetSignalChannel(ctx, "wake").Receive(ctx, nil); err != nil {
			return err
		}
		order = append(order, "workflow")
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "updatable", nil))

	completion := worker.handleActivation(activation(
		"run-1",
		signalJob(t, worker, "wake"),
		updateJob(t, worker, "update-1", "protocol-1", "record", true),
		queryJob(t, worker, "query-1", "order"),
	))
	if got := queryValue[[]string](t, worker, completion); strings.Join(got, ",") != "update,workflow" {
		t.Fatalf("execution order = %v, want update then workflow", got)
	}
}

func TestUpdateRegistrationReplacesExistingHandler(t *testing.T) {
	worker := newTestWorker()
	worker.Register("updatable", func(ctx *Context) error {
		if err := SetUpdateHandler(ctx, "value", func(*Context) (string, error) { return "old", nil }, UpdateHandlerOptions{}); err != nil {
			return err
		}
		if err := SetUpdateHandler(ctx, "value", func(*Context) (string, error) { return "new", nil }, UpdateHandlerOptions{}); err != nil {
			return err
		}
		return GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "updatable", nil))

	responses := updateResponses(t, worker.handleActivation(activation(
		"run-1",
		updateJob(t, worker, "update-1", "protocol-1", "value", true),
	)))
	if got := updateValue[string](t, worker, responses[1]); got != "new" {
		t.Fatalf("replacement update result = %q", got)
	}
}

func TestUpdateRegistrationValidation(t *testing.T) {
	tests := []struct {
		name       string
		updateName string
		handler    any
		validator  any
		want       string
	}{
		{name: "empty name", handler: func(*Context) error { return nil }, want: "name is required"},
		{name: "reserved name", updateName: "__internal", handler: func(*Context) error { return nil }, want: "reserved"},
		{name: "nil handler", updateName: "update", want: "function is required"},
		{name: "non-function handler", updateName: "update", handler: "handler", want: "must be a function"},
		{name: "missing context", updateName: "update", handler: func() error { return nil }, want: "must accept *workflow.Context first"},
		{name: "invalid handler result", updateName: "update", handler: func(*Context) string { return "" }, want: "must return error"},
		{name: "non-function validator", updateName: "update", handler: func(*Context) error { return nil }, validator: "validator", want: "validator must be a function"},
		{name: "invalid validator result", updateName: "update", handler: func(*Context) error { return nil }, validator: func() bool { return true }, want: "validator must return error"},
		{name: "too many validator results", updateName: "update", handler: func(*Context) error { return nil }, validator: func() (int, error) { return 0, nil }, want: "validator must return error"},
		{name: "parameter count mismatch", updateName: "update", handler: func(*Context, int) error { return nil }, validator: func() error { return nil }, want: "parameters must match"},
		{name: "parameter type mismatch", updateName: "update", handler: func(*Context, int) error { return nil }, validator: func(string) error { return nil }, want: "parameter 0"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := newRegisteredUpdate(testCase.updateName, testCase.handler, testCase.validator)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("newRegisteredUpdate() error = %v, want %q", err, testCase.want)
			}
		})
	}

	for _, validator := range []any{
		func(int) error { return nil },
		func(*Context, int) error { return nil },
	} {
		if _, err := newRegisteredUpdate("valid", func(*Context, int) error { return nil }, validator); err != nil {
			t.Fatalf("valid validator %T rejected: %v", validator, err)
		}
	}
}

func updateJob(
	t *testing.T,
	worker *Worker,
	updateID, protocolInstanceID, name string,
	runValidator bool,
	arguments ...any,
) *workflowactivation.WorkflowActivationJob {
	t.Helper()
	encoded, err := worker.payloadConverter.ToPayloads(arguments...)
	if err != nil {
		t.Fatal(err)
	}
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_DoUpdate{
			DoUpdate: &workflowactivation.DoUpdate{
				Id:                 updateID,
				ProtocolInstanceId: protocolInstanceID,
				Name:               name,
				Input:              encoded.GetPayloads(),
				RunValidator:       runValidator,
			},
		},
	}
}

func updateResponses(
	t *testing.T,
	completion *workflowcompletion.WorkflowActivationCompletion,
) []*workflowcommands.UpdateResponse {
	t.Helper()
	commands := successfulCommands(t, completion)
	responses := make([]*workflowcommands.UpdateResponse, 0, len(commands))
	for _, command := range commands {
		if response := command.GetUpdateResponse(); response != nil {
			responses = append(responses, response)
		}
	}
	return responses
}

func updateValue[T any](t *testing.T, worker *Worker, response *workflowcommands.UpdateResponse) T {
	t.Helper()
	var value T
	if err := worker.payloadConverter.FromPayload(response.GetCompleted(), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

type countingPayloadConverter struct {
	converter.PayloadConverter
	fromPayloadsCalls int
}

func (c *countingPayloadConverter) FromPayloads(payloads *commonpb.Payloads, valuePointers ...any) error {
	c.fromPayloadsCalls++
	return c.PayloadConverter.FromPayloads(payloads, valuePointers...)
}
