package workflowworker

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
	workflowactivation "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowactivation"
	workflowcompletion "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcompletion"
	commonpb "go.temporal.io/api/common/v1"
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
		t.Fatalf("initial activation emitted %d commands, want none while blocked on a signal", len(commands))
	}

	signalled := worker.handleActivation(activation("run-1", signalJob(t, worker, "greet", "Temporal")))
	if got := workflowResult[string](t, worker, signalled); got != "Hello, Temporal" {
		t.Fatalf("workflow result is %q", got)
	}
}

func TestSignalDeliveredBeforeReceiveIsBuffered(t *testing.T) {
	worker := newTestWorker()
	worker.Register("buffered", func(ctx *Context) (string, error) {
		// The timer forces the run to yield so the signal lands before the channel is created.
		if err := ctx.sleep(time.Second); err != nil {
			return "", err
		}
		var name string
		if err := GetSignalChannel(ctx, "greet").Receive(ctx, &name); err != nil {
			return "", err
		}
		return "Hello, " + name, nil
	})

	worker.handleActivation(initializeActivation("run-1", "buffered", nil))
	buffered := worker.handleActivation(activation("run-1", signalJob(t, worker, "greet", "Temporal")))
	if commands := successfulCommands(t, buffered); len(commands) != 0 {
		t.Fatalf("buffering activation emitted %d commands, want none while the timer is pending", len(commands))
	}

	fired := worker.handleActivation(activation("run-1", fireTimerJob(1)))
	if got := workflowResult[string](t, worker, fired); got != "Hello, Temporal" {
		t.Fatalf("workflow result is %q", got)
	}
}

func TestSignalsAreReceivedInDeliveryOrder(t *testing.T) {
	worker := newTestWorker()
	worker.Register("ordered", func(ctx *Context) (string, error) {
		channel := GetSignalChannel(ctx, "greet")
		var received []string
		for len(received) < 3 {
			var name string
			if err := channel.Receive(ctx, &name); err != nil {
				return "", err
			}
			received = append(received, name)
		}
		return strings.Join(received, ","), nil
	})

	worker.handleActivation(initializeActivation("run-1", "ordered", nil))
	completion := worker.handleActivation(activation(
		"run-1",
		signalJob(t, worker, "greet", "one"),
		signalJob(t, worker, "greet", "two"),
		signalJob(t, worker, "greet", "three"),
	))
	if got := workflowResult[string](t, worker, completion); got != "one,two,three" {
		t.Fatalf("signals were received as %q", got)
	}
}

func TestSignalChannelsAreSeparatedByName(t *testing.T) {
	worker := newTestWorker()
	worker.Register("named", func(ctx *Context) (string, error) {
		var second string
		if err := GetSignalChannel(ctx, "second").Receive(ctx, &second); err != nil {
			return "", err
		}
		var first string
		if err := GetSignalChannel(ctx, "first").Receive(ctx, &first); err != nil {
			return "", err
		}
		return first + "/" + second, nil
	})

	worker.handleActivation(initializeActivation("run-1", "named", nil))
	completion := worker.handleActivation(activation(
		"run-1",
		signalJob(t, worker, "first", "one"),
		signalJob(t, worker, "second", "two"),
	))
	if got := workflowResult[string](t, worker, completion); got != "one/two" {
		t.Fatalf("signals were received as %q", got)
	}
}

func TestSignalChannelIsSharedAcrossCoroutines(t *testing.T) {
	worker := newTestWorker()
	worker.Register("shared", func(ctx *Context) (string, error) {
		firstDone, firstSettable := NewFuture(ctx)
		secondDone, secondSettable := NewFuture(ctx)
		receive := func(settable Settable) func(*Context) {
			return func(ctx *Context) {
				var name string
				if err := GetSignalChannel(ctx, "greet").Receive(ctx, &name); err != nil {
					settable.SetError(err)
					return
				}
				settable.SetValue(name)
			}
		}
		GoNamed(ctx, "first-receiver", receive(firstSettable))
		GoNamed(ctx, "second-receiver", receive(secondSettable))

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
	// Each signal reaches exactly one receiver, oldest waiter first.
	if got := workflowResult[string](t, worker, completion); got != "one/two" {
		t.Fatalf("shared channel delivered %q", got)
	}
}

func TestSignalReceiveAsyncReportsBufferedSignals(t *testing.T) {
	worker := newTestWorker()
	worker.Register("drain", func(ctx *Context) (string, error) {
		if err := ctx.sleep(time.Second); err != nil {
			return "", err
		}
		channel := GetSignalChannel(ctx, "greet")
		if channel.Len() != 2 {
			return "", fmt.Errorf("channel buffered %d signals, want 2", channel.Len())
		}
		var drained []string
		for {
			var name string
			ok, err := channel.ReceiveAsync(&name)
			if err != nil {
				return "", err
			}
			if !ok {
				break
			}
			drained = append(drained, name)
		}
		if channel.Len() != 0 {
			return "", fmt.Errorf("channel retained %d signals after draining", channel.Len())
		}
		return strings.Join(drained, ","), nil
	})

	worker.handleActivation(initializeActivation("run-1", "drain", nil))
	worker.handleActivation(activation(
		"run-1",
		signalJob(t, worker, "greet", "one"),
		signalJob(t, worker, "greet", "two"),
	))
	completion := worker.handleActivation(activation("run-1", fireTimerJob(1)))
	if got := workflowResult[string](t, worker, completion); got != "one,two" {
		t.Fatalf("drained signals are %q", got)
	}
}

func TestSignalWithoutInputDecodesZeroValue(t *testing.T) {
	worker := newTestWorker()
	worker.Register("empty", func(ctx *Context) (string, error) {
		name := "unchanged"
		if err := GetSignalChannel(ctx, "wake").Receive(ctx, &name); err != nil {
			return "", err
		}
		return name, nil
	})

	worker.handleActivation(initializeActivation("run-1", "empty", nil))
	completion := worker.handleActivation(activation("run-1", &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_SignalWorkflow{
			SignalWorkflow: &workflowactivation.SignalWorkflow{SignalName: "wake"},
		},
	}))
	if got := workflowResult[string](t, worker, completion); got != "unchanged" {
		t.Fatalf("signal without input decoded %q, want the target left untouched", got)
	}
}

func TestSignalReceiveAcceptsNilTargetAndRejectsNonPointers(t *testing.T) {
	worker := newTestWorker()
	worker.Register("discard", func(ctx *Context) (string, error) {
		channel := GetSignalChannel(ctx, "greet")
		if _, err := channel.ReceiveAsync("not-a-pointer"); err == nil ||
			!strings.Contains(err.Error(), "non-nil pointer") {
			return "", fmt.Errorf("ReceiveAsync error = %v, want a pointer requirement", err)
		}
		if err := channel.Receive(ctx, nil); err != nil {
			return "", err
		}
		return "discarded", nil
	})

	worker.handleActivation(initializeActivation("run-1", "discard", nil))
	completion := worker.handleActivation(activation("run-1", signalJob(t, worker, "greet", "Temporal")))
	if got := workflowResult[string](t, worker, completion); got != "discarded" {
		t.Fatalf("workflow result is %q", got)
	}
}

func TestSignalDecodeFailureFailsWorkflow(t *testing.T) {
	worker := newTestWorker()
	worker.Register("mistyped", func(ctx *Context) error {
		var count int
		return GetSignalChannel(ctx, "greet").Receive(ctx, &count)
	})

	worker.handleActivation(initializeActivation("run-1", "mistyped", nil))
	completion := worker.handleActivation(activation("run-1", signalJob(t, worker, "greet", "Temporal")))
	command := onlyCommand(t, completion).GetFailWorkflowExecution()
	if command == nil {
		t.Fatalf("signal decode command is %T, want FailWorkflowExecution", onlyCommand(t, completion).Variant)
	}
	if !strings.Contains(failureMessage(command.Failure), `decode signal "greet"`) {
		t.Fatalf("signal decode failure = %q", failureMessage(command.Failure))
	}
}

func TestSignalForUnreceivedNameDoesNotFailActivation(t *testing.T) {
	worker := newTestWorker()
	worker.Register("ignoring", func(ctx *Context) error {
		return ctx.sleep(time.Second)
	})

	worker.handleActivation(initializeActivation("run-1", "ignoring", nil))
	completion := worker.handleActivation(activation("run-1", signalJob(t, worker, "unknown", "Temporal")))
	if commands := successfulCommands(t, completion); len(commands) != 0 {
		t.Fatalf("unhandled signal emitted %d commands, want none", len(commands))
	}
	if buffered := worker.getRun("run-1").signalChannel("unknown").Len(); buffered != 1 {
		t.Fatalf("unhandled signal buffered %d values, want 1", buffered)
	}
}

func TestSignalDeliveredWithInitializationIsReceived(t *testing.T) {
	worker := newTestWorker()
	worker.Register("immediate", func(ctx *Context) (string, error) {
		var name string
		if err := GetSignalChannel(ctx, "greet").Receive(ctx, &name); err != nil {
			return "", err
		}
		return "Hello, " + name, nil
	})

	initialization := initializeActivation("run-1", "immediate", nil)
	initialization.Jobs = append(initialization.Jobs, signalJob(t, worker, "greet", "Temporal"))
	completion := worker.handleActivation(initialization)
	if got := workflowResult[string](t, worker, completion); got != "Hello, Temporal" {
		t.Fatalf("workflow result is %q", got)
	}
}

func TestSignalReceiveRejectsForeignContext(t *testing.T) {
	execution := newWorkflowExecution(converter.GetDefaultPayloadConverter(), "namespace", "queue", "run-1")
	other := newWorkflowExecution(converter.GetDefaultPayloadConverter(), "namespace", "queue", "run-2")
	channel := execution.signalChannel("greet")

	var name string
	err := channel.Receive(&Context{execution: other}, &name)
	if err == nil || !strings.Contains(err.Error(), "requires its workflow context") {
		t.Fatalf("Receive error = %v, want a workflow context requirement", err)
	}
}

func TestSignalWaiterIsReleasedWhenExecutionStops(t *testing.T) {
	worker := newTestWorker()
	worker.Register("evicted", func(ctx *Context) error {
		return GetSignalChannel(ctx, "greet").Receive(ctx, nil)
	})

	worker.handleActivation(initializeActivation("run-1", "evicted", nil))
	execution := worker.getRun("run-1")
	if waiting := len(execution.signalChannel("greet").waiters); waiting != 1 {
		t.Fatalf("signal channel has %d waiters, want 1", waiting)
	}

	worker.handleActivation(activation("run-1", &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_RemoveFromCache{
			RemoveFromCache: &workflowactivation.RemoveFromCache{
				Reason: workflowactivation.RemoveFromCache_LANG_REQUESTED,
			},
		},
	}))
	if run := worker.getRun("run-1"); run != nil {
		t.Fatal("evicted run is still cached")
	}
}

func signalJob(t *testing.T, worker *Worker, name string, arguments ...any) *workflowactivation.WorkflowActivationJob {
	t.Helper()
	var input []*commonpb.Payload
	if len(arguments) > 0 {
		encoded, err := worker.payloadConverter.ToPayloads(arguments...)
		if err != nil {
			t.Fatal(err)
		}
		input = encoded.GetPayloads()
	}
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_SignalWorkflow{
			SignalWorkflow: &workflowactivation.SignalWorkflow{
				SignalName: name,
				Input:      input,
				Identity:   "test-client",
			},
		},
	}
}

func workflowResult[T any](
	t *testing.T,
	worker *Worker,
	completion *workflowcompletion.WorkflowActivationCompletion,
) T {
	t.Helper()
	command := onlyCommand(t, completion)
	complete := command.GetCompleteWorkflowExecution()
	if complete == nil {
		t.Fatalf("completion command is %T, want CompleteWorkflowExecution", command.Variant)
	}
	var result T
	if err := worker.payloadConverter.FromPayload(complete.Result, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
