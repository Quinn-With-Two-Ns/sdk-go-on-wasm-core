package workflowworker

import (
	"fmt"
	"strings"
	"testing"
	"time"

	workflowactivation "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowactivation"
	workflowcompletion "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcompletion"
)

func TestSignalResumesBlockedWorkflow(t *testing.T) {
	worker := newTestWorker()
	worker.Register("signalled", func(ctx *Context) (string, error) {
		var name string
		if err := GetSignalChannel(ctx, "greet").Receive(ctx, &name); err != nil {
			return "", err
		}
		return "Hello, " + name, nil
	})

	initialized := worker.handleActivation(initializeActivation("run-1", "signalled", nil))
	if commands := successfulCommands(t, initialized); len(commands) != 0 {
		t.Fatalf("initial activation emitted %d commands while waiting for a signal", len(commands))
	}

	signalled := worker.handleActivation(activation("run-1", signalJob(t, worker, "greet", "Temporal")))
	if got := signalWorkflowResult[string](t, worker, signalled); got != "Hello, Temporal" {
		t.Fatalf("workflow result = %q", got)
	}
}

func TestSignalIsVisibleBeforeFirstCoroutineIteration(t *testing.T) {
	worker := newTestWorker()
	worker.Register("immediate", func(ctx *Context) (string, error) {
		var value string
		ok, err := GetSignalChannel(ctx, "greet").ReceiveAsync(&value)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", fmt.Errorf("signal was not delivered before workflow iteration")
		}
		return value, nil
	})

	initialization := initializeActivation("run-1", "immediate", nil)
	initialization.Jobs = append(initialization.Jobs, signalJob(t, worker, "greet", "Temporal"))
	completion := worker.handleActivation(initialization)
	if got := signalWorkflowResult[string](t, worker, completion); got != "Temporal" {
		t.Fatalf("workflow result = %q", got)
	}
}

func TestSignalDeliveredBeforeChannelLookupIsBuffered(t *testing.T) {
	worker := newTestWorker()
	worker.Register("buffered", func(ctx *Context) (string, error) {
		if err := Sleep(ctx, time.Second); err != nil {
			return "", err
		}
		var name string
		if err := GetSignalChannel(ctx, "greet").Receive(ctx, &name); err != nil {
			return "", err
		}
		return name, nil
	})

	worker.handleActivation(initializeActivation("run-1", "buffered", nil))
	buffered := worker.handleActivation(activation("run-1", signalJob(t, worker, "greet", "Temporal")))
	if commands := successfulCommands(t, buffered); len(commands) != 0 {
		t.Fatalf("signal activation emitted %d commands while timer was pending", len(commands))
	}

	completed := worker.handleActivation(activation("run-1", fireTimerJob(1)))
	if got := signalWorkflowResult[string](t, worker, completed); got != "Temporal" {
		t.Fatalf("workflow result = %q", got)
	}
}

func TestSignalChannelsPreserveNameAndDeliveryOrder(t *testing.T) {
	worker := newTestWorker()
	worker.Register("ordered", func(ctx *Context) (string, error) {
		greetings := GetSignalChannel(ctx, "greet")
		var received []string
		for range 2 {
			var value string
			if err := greetings.Receive(ctx, &value); err != nil {
				return "", err
			}
			received = append(received, value)
		}
		var exit string
		if err := GetSignalChannel(ctx, "exit").Receive(ctx, &exit); err != nil {
			return "", err
		}
		return strings.Join(received, ",") + "/" + exit, nil
	})

	worker.handleActivation(initializeActivation("run-1", "ordered", nil))
	completion := worker.handleActivation(activation(
		"run-1",
		signalJob(t, worker, "exit", "done"),
		signalJob(t, worker, "greet", "one"),
		signalJob(t, worker, "greet", "two"),
	))
	if got := signalWorkflowResult[string](t, worker, completion); got != "one,two/done" {
		t.Fatalf("signals received as %q", got)
	}
}

func TestSignalChannelSharesSignalsAcrossWaitingCoroutines(t *testing.T) {
	worker := newTestWorker()
	worker.Register("shared", func(ctx *Context) (string, error) {
		firstDone, firstSettable := NewFuture(ctx)
		secondDone, secondSettable := NewFuture(ctx)
		receive := func(settable Settable) func(*Context) {
			return func(ctx *Context) {
				var value string
				if err := GetSignalChannel(ctx, "greet").Receive(ctx, &value); err != nil {
					settable.SetError(err)
					return
				}
				settable.SetValue(value)
			}
		}
		GoNamed(ctx, "first", receive(firstSettable))
		GoNamed(ctx, "second", receive(secondSettable))

		var first, second string
		if err := firstDone.Get(ctx, &first); err != nil {
			return "", err
		}
		if err := secondDone.Get(ctx, &second); err != nil {
			return "", err
		}
		return first + "/" + second, nil
	})

	worker.handleActivation(initializeActivation("run-1", "shared", nil))
	completion := worker.handleActivation(activation(
		"run-1",
		signalJob(t, worker, "greet", "one"),
		signalJob(t, worker, "greet", "two"),
	))
	if got := signalWorkflowResult[string](t, worker, completion); got != "one/two" {
		t.Fatalf("shared channel delivered %q", got)
	}
}

func TestSignalChannelReceiveAsyncAndLen(t *testing.T) {
	worker := newTestWorker()
	worker.Register("drain", func(ctx *Context) (string, error) {
		channel := GetSignalChannel(ctx, "greet")
		if channel.Len() != 2 {
			return "", fmt.Errorf("buffer length = %d, want 2", channel.Len())
		}
		var values []string
		for {
			var value string
			ok, err := channel.ReceiveAsync(&value)
			if err != nil {
				return "", err
			}
			if !ok {
				break
			}
			values = append(values, value)
		}
		if channel.Len() != 0 {
			return "", fmt.Errorf("buffer length after drain = %d", channel.Len())
		}
		return strings.Join(values, ","), nil
	})

	initialization := initializeActivation("run-1", "drain", nil)
	initialization.Jobs = append(
		initialization.Jobs,
		signalJob(t, worker, "greet", "one"),
		signalJob(t, worker, "greet", "two"),
	)
	completion := worker.handleActivation(initialization)
	if got := signalWorkflowResult[string](t, worker, completion); got != "one,two" {
		t.Fatalf("drained signals = %q", got)
	}
}

func TestSignalWithoutInputLeavesUntypedTargetUnchanged(t *testing.T) {
	worker := newTestWorker()
	worker.Register("empty", func(ctx *Context) (string, error) {
		value := "unchanged"
		if err := GetSignalChannel(ctx, "wake").Receive(ctx, &value); err != nil {
			return "", err
		}
		return value, nil
	})

	worker.handleActivation(initializeActivation("run-1", "empty", nil))
	completion := worker.handleActivation(activation("run-1", signalJob(t, worker, "wake")))
	if got := signalWorkflowResult[string](t, worker, completion); got != "unchanged" {
		t.Fatalf("untyped empty signal changed target to %q", got)
	}
}

func TestSignalDecodeFailureFailsWorkflow(t *testing.T) {
	worker := newTestWorker()
	worker.Register("mistyped", func(ctx *Context) error {
		var count int
		return GetSignalChannel(ctx, "count").Receive(ctx, &count)
	})

	worker.handleActivation(initializeActivation("run-1", "mistyped", nil))
	completion := worker.handleActivation(activation("run-1", signalJob(t, worker, "count", "not-an-int")))
	command := onlyCommand(t, completion).GetFailWorkflowExecution()
	if command == nil || !strings.Contains(failureMessage(command.GetFailure()), `decode signal "count"`) {
		t.Fatalf("decode failure command = %+v", command)
	}
}

func TestUnobservedSignalIsBuffered(t *testing.T) {
	worker := newTestWorker()
	worker.Register("ignoring", func(ctx *Context) error { return Sleep(ctx, time.Second) })

	worker.handleActivation(initializeActivation("run-1", "ignoring", nil))
	completion := worker.handleActivation(activation("run-1", signalJob(t, worker, "unknown", "value")))
	if commands := successfulCommands(t, completion); len(commands) != 0 {
		t.Fatalf("unobserved signal emitted %d commands", len(commands))
	}
	if buffered := worker.getRun("run-1").signalChannel("unknown").Len(); buffered != 1 {
		t.Fatalf("buffered signals = %d, want 1", buffered)
	}
}

func TestSignalDeliveryReplaysAfterCacheEviction(t *testing.T) {
	worker := newTestWorker()
	worker.Register("replayed", func(ctx *Context) (string, error) {
		var value string
		if err := GetSignalChannel(ctx, "greet").Receive(ctx, &value); err != nil {
			return "", err
		}
		return value, nil
	})

	worker.handleActivation(initializeActivation("run-1", "replayed", nil))
	worker.handleActivation(activation("run-1", &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_RemoveFromCache{
			RemoveFromCache: &workflowactivation.RemoveFromCache{
				Reason: workflowactivation.RemoveFromCache_LANG_REQUESTED,
			},
		},
	}))
	if worker.getRun("run-1") != nil {
		t.Fatal("workflow remained cached after eviction")
	}

	replay := initializeActivation("run-1", "replayed", nil)
	replay.Jobs = append(replay.Jobs, signalJob(t, worker, "greet", "replayed-value"))
	completion := worker.handleActivation(replay)
	if got := signalWorkflowResult[string](t, worker, completion); got != "replayed-value" {
		t.Fatalf("replayed signal delivered %q", got)
	}
}

func signalJob(t *testing.T, worker *Worker, name string, arguments ...any) *workflowactivation.WorkflowActivationJob {
	t.Helper()
	encoded, err := worker.payloadConverter.ToPayloads(arguments...)
	if err != nil {
		t.Fatal(err)
	}
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_SignalWorkflow{
			SignalWorkflow: &workflowactivation.SignalWorkflow{
				SignalName: name,
				Input:      encoded.GetPayloads(),
				Identity:   "test-client",
			},
		},
	}
}

func signalWorkflowResult[T any](
	t *testing.T,
	worker *Worker,
	completion *workflowcompletion.WorkflowActivationCompletion,
) T {
	t.Helper()
	command := onlyCommand(t, completion).GetCompleteWorkflowExecution()
	if command == nil {
		t.Fatalf("completion command is not CompleteWorkflowExecution")
	}
	var result T
	if err := worker.payloadConverter.FromPayload(command.Result, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
