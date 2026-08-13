package workflowworker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
	activityresult "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/activityresult"
	childworkflow "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/childworkflow"
	workflowactivation "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowactivation"
	workflowcommands "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcommands"
	workflowcompletion "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcompletion"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/proto"
)

func TestTimerWorkflowCompletesWithTypedResult(t *testing.T) {
	worker := newTestWorker()
	worker.Register("timer", func(ctx *Context, name string) (string, error) {
		if err := ctx.sleep(2 * time.Second); err != nil {
			return "", err
		}
		return "Hello, " + name, nil
	})
	input, err := worker.payloadConverter.ToPayloads("Temporal")
	if err != nil {
		t.Fatal(err)
	}

	initialized := worker.handleActivation(initializeActivation("run-1", "timer", input.Payloads))
	startTimer := onlyCommand(t, initialized).GetStartTimer()
	if startTimer == nil {
		t.Fatalf("initial command is %T, want StartTimer", onlyCommand(t, initialized).Variant)
	}
	if startTimer.Seq != 1 || startTimer.StartToFireTimeout.AsDuration() != 2*time.Second {
		t.Fatalf("timer is sequence %d for %s", startTimer.Seq, startTimer.StartToFireTimeout.AsDuration())
	}

	fired := worker.handleActivation(activation("run-1", fireTimerJob(1)))
	complete := onlyCommand(t, fired).GetCompleteWorkflowExecution()
	if complete == nil {
		t.Fatalf("timer resolution command is %T, want CompleteWorkflowExecution", onlyCommand(t, fired).Variant)
	}
	var result string
	if err := worker.payloadConverter.FromPayload(complete.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result != "Hello, Temporal" {
		t.Fatalf("workflow result is %q", result)
	}
}

func TestRegisteredWorkflowConvertsTypedInputAndResult(t *testing.T) {
	payloadConverter := converter.GetDefaultPayloadConverter()
	input, err := payloadConverter.ToPayloads("Temporal", 3)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := newRegisteredWorkflow("greet", func(_ *Context, name string, count int) (string, error) {
		return strings.Repeat("Hello, "+name+"!", count), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := handler.execute(&Context{}, payloadConverter, input.Payloads)
	if err != nil {
		t.Fatal(err)
	}
	var decoded string
	if err := payloadConverter.FromPayload(result, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != "Hello, Temporal!Hello, Temporal!Hello, Temporal!" {
		t.Fatalf("workflow result = %q", decoded)
	}
}

func TestRegisteredWorkflowRejectsInvalidSignatures(t *testing.T) {
	testCases := []struct {
		name     string
		function any
		want     string
	}{
		{name: "nil", function: nil, want: "function is required"},
		{name: "non-function", function: "workflow", want: "must be a function"},
		{name: "missing context", function: func() error { return nil }, want: "must accept *workflow.Context first"},
		{name: "missing error", function: func(*Context) string { return "" }, want: "must return error or (result, error)"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := newRegisteredWorkflow("test", testCase.function)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("newRegisteredWorkflow error = %v, want substring %q", err, testCase.want)
			}
		})
	}
}

func TestTimerAndActivityCanBeStartedTogether(t *testing.T) {
	worker := newTestWorker()
	worker.Register("timer-and-activity", func(ctx *Context) error {
		timer := ctx.newTimer(time.Second)
		activity := ctx.executeActivity(ActivityOptions{
			ActivityType:        "work",
			StartToCloseTimeout: time.Minute,
		})
		if err := timer.Get(ctx, nil); err != nil {
			return err
		}
		return activity.Get(ctx, nil)
	})

	commands := successfulCommands(t, worker.handleActivation(
		initializeActivation("run-1", "timer-and-activity", nil),
	))
	if len(commands) != 2 || commands[0].GetStartTimer() == nil || commands[1].GetScheduleActivity() == nil {
		t.Fatalf("timer and activity were not issued together: %v", commands)
	}
	result, _ := worker.payloadConverter.ToPayload("done")
	completion := worker.handleActivation(activation("run-1", resolveActivityJob(2, result), fireTimerJob(1)))
	if onlyCommand(t, completion).GetCompleteWorkflowExecution() == nil {
		t.Fatal("timer and activity workflow did not complete")
	}
}

func TestLocalActivityCommandAndResolution(t *testing.T) {
	worker := newTestWorker()
	worker.Register("local", func(ctx *Context, name string) (string, error) {
		returnValue := ctx.executeLocalActivity(LocalActivityOptions{
			ActivityType: "greet", ScheduleToCloseTimeout: time.Minute,
			ScheduleToStartTimeout: time.Second, StartToCloseTimeout: 30 * time.Second,
			LocalRetryThreshold: 10 * time.Second,
		}, name)
		var result string
		return result, returnValue.Get(ctx, &result)
	})
	input, _ := worker.payloadConverter.ToPayloads("Temporal")
	commands := successfulCommands(t, worker.handleActivation(initializeActivation("run-1", "local", input.Payloads)))
	if len(commands) != 1 {
		t.Fatalf("local activity commands = %d, want 1", len(commands))
	}
	schedule := commands[0].GetScheduleLocalActivity()
	if schedule == nil || schedule.ActivityType != "greet" || schedule.ActivityId != "local-activity-1" || schedule.Attempt != 1 {
		t.Fatalf("unexpected local activity command: %v", schedule)
	}
	if schedule.ScheduleToCloseTimeout.AsDuration() != time.Minute ||
		schedule.ScheduleToStartTimeout.AsDuration() != time.Second ||
		schedule.StartToCloseTimeout.AsDuration() != 30*time.Second ||
		schedule.LocalRetryThreshold.AsDuration() != 10*time.Second {
		t.Fatalf("local activity timeouts were not preserved: %v", schedule)
	}
	result, _ := worker.payloadConverter.ToPayload("Hello")
	job := resolveActivityJob(1, result)
	job.GetResolveActivity().IsLocal = true
	completion := worker.handleActivation(activation("run-1", job))
	if onlyCommand(t, completion).GetCompleteWorkflowExecution() == nil {
		t.Fatal("local activity workflow did not complete")
	}
}

func TestRestartReplayRegeneratesTheSameCommands(t *testing.T) {
	register := func(worker *Worker) {
		worker.Register("timer", func(ctx *Context) (int, error) {
			if err := ctx.sleep(time.Second); err != nil {
				return 0, err
			}
			return 42, nil
		})
	}

	live := newTestWorker()
	register(live)
	first := successfulCommands(t, live.handleActivation(initializeActivation("run-1", "timer", nil)))
	second := successfulCommands(t, live.handleActivation(activation("run-1", fireTimerJob(1))))
	want := append(append([]*workflowcommands.WorkflowCommand{}, first...), second...)

	restarted := newTestWorker()
	register(restarted)
	replay := initializeActivation("run-1", "timer", nil)
	replay.IsReplaying = true
	replay.Jobs = append(replay.Jobs, fireTimerJob(1))
	got := successfulCommands(t, restarted.handleActivation(replay))
	if len(got) != len(want) {
		t.Fatalf("replay produced %d commands, want %d", len(got), len(want))
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("replay command %d differs:\n got %v\nwant %v", i, got[i], want[i])
		}
	}
}

func TestRemoveFromCacheStopsAndForgetsRun(t *testing.T) {
	worker := newTestWorker()
	worker.Register("timer", func(ctx *Context) error { return ctx.sleep(time.Hour) })
	initialized := worker.handleActivation(initializeActivation("run-1", "timer", nil))
	if onlyCommand(t, initialized).GetStartTimer() == nil {
		t.Fatal("workflow did not start its timer")
	}
	execution := worker.runs["run-1"]

	evicted := worker.handleActivation(activation("run-1", &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_RemoveFromCache{
			RemoveFromCache: &workflowactivation.RemoveFromCache{
				Reason: workflowactivation.RemoveFromCache_LANG_REQUESTED,
			},
		},
	}))
	if len(successfulCommands(t, evicted)) != 0 {
		t.Fatal("cache eviction emitted workflow commands")
	}
	if worker.runs["run-1"] != nil {
		t.Fatal("cache eviction retained the workflow run")
	}
	if !execution.stopped {
		t.Fatal("cache eviction did not stop the workflow execution")
	}

	replayed := worker.handleActivation(initializeActivation("run-1", "timer", nil))
	if onlyCommand(t, replayed).GetStartTimer() == nil {
		t.Fatal("evicted workflow could not be replayed from InitializeWorkflow")
	}
}

func TestWorkflowSchedulesAndResolvesActivity(t *testing.T) {
	worker := newTestWorker()
	worker.Register("activity", func(ctx *Context, name string) (string, error) {
		if err := ctx.sleep(time.Second); err != nil {
			return "", err
		}
		var greeting string
		err := ctx.executeActivity(ActivityOptions{
			ActivityType:        "greet",
			StartToCloseTimeout: 10 * time.Second,
		}, name).Get(ctx, &greeting)
		return greeting, err
	})
	input, err := worker.payloadConverter.ToPayloads("Temporal")
	if err != nil {
		t.Fatal(err)
	}
	worker.handleActivation(initializeActivation("run-1", "activity", input.Payloads))

	afterTimer := worker.handleActivation(activation("run-1", fireTimerJob(1)))
	schedule := onlyCommand(t, afterTimer).GetScheduleActivity()
	if schedule == nil {
		t.Fatalf("timer resolution command is %T, want ScheduleActivity", onlyCommand(t, afterTimer).Variant)
	}
	if schedule.Seq != 2 || schedule.ActivityId != "activity-2" || schedule.ActivityType != "greet" {
		t.Fatalf("unexpected activity command: %v", schedule)
	}
	if schedule.TaskQueue != "test-queue" || schedule.DoNotEagerlyExecute {
		t.Fatalf("activity routing is queue %q, eager=%v", schedule.TaskQueue, !schedule.DoNotEagerlyExecute)
	}
	var activityInput string
	if err := worker.payloadConverter.FromPayloads(payloads(schedule.Arguments), &activityInput); err != nil {
		t.Fatal(err)
	}
	if activityInput != "Temporal" {
		t.Fatalf("activity input is %q", activityInput)
	}

	activityOutput, err := worker.payloadConverter.ToPayload("Hello, Temporal")
	if err != nil {
		t.Fatal(err)
	}
	resolved := worker.handleActivation(activation("run-1", &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_ResolveActivity{
			ResolveActivity: &workflowactivation.ResolveActivity{
				Seq: 2,
				Result: &activityresult.ActivityResolution{
					Status: &activityresult.ActivityResolution_Completed{
						Completed: &activityresult.Success{Result: activityOutput},
					},
				},
			},
		},
	}))
	complete := onlyCommand(t, resolved).GetCompleteWorkflowExecution()
	if complete == nil {
		t.Fatalf("activity resolution command is %T, want CompleteWorkflowExecution", onlyCommand(t, resolved).Variant)
	}
	var result string
	if err := worker.payloadConverter.FromPayload(complete.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result != "Hello, Temporal" {
		t.Fatalf("workflow result is %q", result)
	}
}

func TestWorkflowCanDisableEagerActivityExecution(t *testing.T) {
	worker := newTestWorker()
	worker.Register("disable-eager", func(ctx *Context) error {
		return ctx.executeActivity(ActivityOptions{
			ActivityType:          "greet",
			StartToCloseTimeout:   time.Second,
			DisableEagerExecution: true,
		}).Get(ctx, nil)
	})

	completion := worker.handleActivation(initializeActivation("run-disable-eager", "disable-eager", nil))
	schedule := onlyCommand(t, completion).GetScheduleActivity()
	if schedule == nil || !schedule.DoNotEagerlyExecute {
		t.Fatalf("schedule = %v, want eager execution disabled", schedule)
	}
}

func TestParallelActivitiesEmitCommandsBeforeEitherIsAwaited(t *testing.T) {
	worker := newTestWorker()
	worker.Register("parallel-activities", func(ctx *Context) (string, error) {
		first := ctx.executeActivity(ActivityOptions{
			ActivityType:        "first",
			StartToCloseTimeout: time.Minute,
		}, "one")
		second := ctx.executeActivity(ActivityOptions{
			ActivityType:        "second",
			StartToCloseTimeout: time.Minute,
		}, "two")
		var firstResult, secondResult string
		if err := first.Get(ctx, &firstResult); err != nil {
			return "", err
		}
		if err := second.Get(ctx, &secondResult); err != nil {
			return "", err
		}
		return firstResult + "|" + secondResult, nil
	})

	commands := successfulCommands(t, worker.handleActivation(
		initializeActivation("run-1", "parallel-activities", nil),
	))
	if len(commands) != 2 {
		t.Fatalf("initial completion has %d commands, want 2 parallel activities", len(commands))
	}
	first := commands[0].GetScheduleActivity()
	second := commands[1].GetScheduleActivity()
	if first == nil || second == nil || first.Seq != 1 || second.Seq != 2 ||
		first.ActivityType != "first" || second.ActivityType != "second" {
		t.Fatalf("parallel activity commands are not deterministic: %v", commands)
	}

	firstPayload, err := worker.payloadConverter.ToPayload("one-result")
	if err != nil {
		t.Fatal(err)
	}
	secondPayload, err := worker.payloadConverter.ToPayload("two-result")
	if err != nil {
		t.Fatal(err)
	}
	completion := worker.handleActivation(activation(
		"run-1",
		resolveActivityJob(2, secondPayload),
		resolveActivityJob(1, firstPayload),
	))
	complete := onlyCommand(t, completion).GetCompleteWorkflowExecution()
	if complete == nil {
		t.Fatalf("parallel activity resolution emitted %T, want CompleteWorkflowExecution", onlyCommand(t, completion).Variant)
	}
	var result string
	if err := worker.payloadConverter.FromPayload(complete.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result != "one-result|two-result" {
		t.Fatalf("parallel activity result = %q", result)
	}
}

func TestParallelActivitiesReplayRegeneratesExactCommandStream(t *testing.T) {
	register := func(worker *Worker) {
		worker.Register("parallel", func(ctx *Context) (string, error) {
			first := ctx.executeActivity(ActivityOptions{
				ActivityType:        "first",
				StartToCloseTimeout: time.Minute,
			})
			second := ctx.executeActivity(ActivityOptions{
				ActivityType:        "second",
				StartToCloseTimeout: time.Minute,
			})
			var firstResult, secondResult string
			if err := first.Get(ctx, &firstResult); err != nil {
				return "", err
			}
			if err := second.Get(ctx, &secondResult); err != nil {
				return "", err
			}
			return firstResult + secondResult, nil
		})
	}
	payloadConverter := converter.GetDefaultPayloadConverter()
	firstPayload, _ := payloadConverter.ToPayload("A")
	secondPayload, _ := payloadConverter.ToPayload("B")

	live := newTestWorker()
	register(live)
	want := append(
		append([]*workflowcommands.WorkflowCommand{}, successfulCommands(t, live.handleActivation(
			initializeActivation("run-1", "parallel", nil),
		))...),
		successfulCommands(t, live.handleActivation(activation(
			"run-1",
			resolveActivityJob(2, secondPayload),
			resolveActivityJob(1, firstPayload),
		)))...,
	)

	restarted := newTestWorker()
	register(restarted)
	replay := initializeActivation("run-1", "parallel", nil)
	replay.IsReplaying = true
	replay.Jobs = append(
		replay.Jobs,
		resolveActivityJob(2, secondPayload),
		resolveActivityJob(1, firstPayload),
	)
	got := successfulCommands(t, restarted.handleActivation(replay))
	if len(got) != len(want) {
		t.Fatalf("parallel replay produced %d commands, want %d", len(got), len(want))
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("parallel replay command %d differs:\n got %v\nwant %v", i, got[i], want[i])
		}
	}
}

func TestWorkflowGoCoroutinesScheduleActivitiesDeterministically(t *testing.T) {
	worker := newTestWorker()
	worker.Register("workflow-go", func(ctx *Context) (string, error) {
		firstDone, firstSettable := NewFuture(ctx)
		secondDone, secondSettable := NewFuture(ctx)
		GoNamed(ctx, "first", func(ctx *Context) {
			var result string
			err := ctx.executeActivity(ActivityOptions{
				ActivityType:        "first",
				StartToCloseTimeout: time.Minute,
			}).Get(ctx, &result)
			firstSettable.Set(result, err)
		})
		GoNamed(ctx, "second", func(ctx *Context) {
			var result string
			err := ctx.executeActivity(ActivityOptions{
				ActivityType:        "second",
				StartToCloseTimeout: time.Minute,
			}).Get(ctx, &result)
			secondSettable.Set(result, err)
		})

		var first, second string
		if err := firstDone.Get(ctx, &first); err != nil {
			return "", err
		}
		if err := secondDone.Get(ctx, &second); err != nil {
			return "", err
		}
		return first + ":" + second, nil
	})

	commands := successfulCommands(t, worker.handleActivation(initializeActivation("run-1", "workflow-go", nil)))
	if len(commands) != 2 {
		t.Fatalf("workflow.Go initialization emitted %d commands, want 2", len(commands))
	}
	if commands[0].GetScheduleActivity().ActivityType != "first" ||
		commands[1].GetScheduleActivity().ActivityType != "second" {
		t.Fatalf("workflow.Go command order is not creation order: %v", commands)
	}

	firstPayload, _ := worker.payloadConverter.ToPayload("A")
	secondPayload, _ := worker.payloadConverter.ToPayload("B")
	completion := worker.handleActivation(activation(
		"run-1",
		resolveActivityJob(2, secondPayload),
		resolveActivityJob(1, firstPayload),
	))
	complete := onlyCommand(t, completion).GetCompleteWorkflowExecution()
	if complete == nil {
		t.Fatal("workflow.Go workflow did not complete")
	}
	var result string
	if err := worker.payloadConverter.FromPayload(complete.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result != "A:B" {
		t.Fatalf("workflow.Go result = %q, want A:B", result)
	}
}

func TestWorkflowGoCoroutinesStartActivitiesAndChildrenInParallel(t *testing.T) {
	worker := newTestWorker()
	worker.Register("mixed-workflow-go", func(ctx *Context) error {
		firstDone, firstSettable := NewFuture(ctx)
		secondDone, secondSettable := NewFuture(ctx)
		start := func(activityType, childType string, done Settable) func(*Context) {
			return func(ctx *Context) {
				activity := ctx.executeActivity(ActivityOptions{
					ActivityType:        activityType,
					StartToCloseTimeout: time.Minute,
				})
				child := ctx.executeChildWorkflow(ChildWorkflowOptions{WorkflowType: childType})
				if err := activity.Get(ctx, nil); err != nil {
					done.SetError(err)
					return
				}
				done.Set(nil, child.Get(ctx, nil))
			}
		}
		GoNamed(ctx, "first", start("activity-one", "child-one", firstSettable))
		GoNamed(ctx, "second", start("activity-two", "child-two", secondSettable))
		if err := firstDone.Get(ctx, nil); err != nil {
			return err
		}
		return secondDone.Get(ctx, nil)
	})

	commands := successfulCommands(t, worker.handleActivation(initializeActivation("run-1", "mixed-workflow-go", nil)))
	if len(commands) != 4 {
		t.Fatalf("mixed workflow.Go initialization emitted %d commands, want 4", len(commands))
	}
	if commands[0].GetScheduleActivity().ActivityType != "activity-one" ||
		commands[1].GetStartChildWorkflowExecution().WorkflowType != "child-one" ||
		commands[2].GetScheduleActivity().ActivityType != "activity-two" ||
		commands[3].GetStartChildWorkflowExecution().WorkflowType != "child-two" {
		t.Fatalf("mixed workflow.Go command order is not deterministic: %v", commands)
	}

	activityResult, _ := worker.payloadConverter.ToPayload("done")
	completion := worker.handleActivation(activation(
		"run-1",
		childWorkflowStartedJob(4, "child-two-run"),
		resolveActivityJob(3, activityResult),
		childWorkflowStartedJob(2, "child-one-run"),
		resolveActivityJob(1, activityResult),
		childWorkflowCompletedJob(4, nil),
		childWorkflowCompletedJob(2, nil),
	))
	if onlyCommand(t, completion).GetCompleteWorkflowExecution() == nil {
		t.Fatal("mixed workflow.Go workflow did not complete")
	}
}

func TestWorkflowStartsAndResolvesChildWorkflow(t *testing.T) {
	worker := newTestWorker()
	worker.Register("parent", func(ctx *Context, name string) (string, error) {
		var greeting string
		err := ctx.executeChildWorkflow(ChildWorkflowOptions{
			WorkflowType:             "greeting-child",
			WorkflowExecutionTimeout: 20 * time.Minute,
			WorkflowRunTimeout:       10 * time.Minute,
			WorkflowTaskTimeout:      30 * time.Second,
			ParentClosePolicy:        enumspb.PARENT_CLOSE_POLICY_ABANDON,
			WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		}, name).Get(ctx, &greeting)
		return greeting, err
	})
	input, err := worker.payloadConverter.ToPayloads("Temporal")
	if err != nil {
		t.Fatal(err)
	}

	initialized := worker.handleActivation(initializeActivation("run-1", "parent", input.Payloads))
	start := onlyCommand(t, initialized).GetStartChildWorkflowExecution()
	if start == nil {
		t.Fatalf("initial command is %T, want StartChildWorkflowExecution", onlyCommand(t, initialized).Variant)
	}
	if start.Seq != 1 || start.WorkflowId != "run-1_1" || start.WorkflowType != "greeting-child" {
		t.Fatalf("unexpected child workflow identity: %v", start)
	}
	if start.TaskQueue != "test-queue" || start.Namespace != "test-namespace" {
		t.Fatalf("child workflow routing is namespace %q, queue %q", start.Namespace, start.TaskQueue)
	}
	if start.WorkflowExecutionTimeout.AsDuration() != 20*time.Minute ||
		start.WorkflowRunTimeout.AsDuration() != 10*time.Minute ||
		start.WorkflowTaskTimeout.AsDuration() != 30*time.Second {
		t.Fatalf("unexpected child workflow timeouts: %v", start)
	}
	if start.ParentClosePolicy != childworkflow.ParentClosePolicy_PARENT_CLOSE_POLICY_ABANDON ||
		start.WorkflowIdReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
		t.Fatalf("unexpected child workflow policies: %v", start)
	}
	var childInput string
	if err := worker.payloadConverter.FromPayloads(payloads(start.Input), &childInput); err != nil {
		t.Fatal(err)
	}
	if childInput != "Temporal" {
		t.Fatalf("child workflow input is %q", childInput)
	}

	started := worker.handleActivation(activation("run-1", childWorkflowStartedJob(1, "child-run-1")))
	if commands := successfulCommands(t, started); len(commands) != 0 {
		t.Fatalf("child start resolution emitted %d commands, want 0", len(commands))
	}

	childOutput, err := worker.payloadConverter.ToPayload("Hello, Temporal")
	if err != nil {
		t.Fatal(err)
	}
	resolved := worker.handleActivation(activation("run-1", childWorkflowCompletedJob(1, childOutput)))
	complete := onlyCommand(t, resolved).GetCompleteWorkflowExecution()
	if complete == nil {
		t.Fatalf("child resolution command is %T, want CompleteWorkflowExecution", onlyCommand(t, resolved).Variant)
	}
	var result string
	if err := worker.payloadConverter.FromPayload(complete.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result != "Hello, Temporal" {
		t.Fatalf("workflow result is %q", result)
	}
}

func TestParallelChildWorkflowsStartBeforeEitherIsAwaited(t *testing.T) {
	worker := newTestWorker()
	worker.Register("parallel-children", func(ctx *Context) (string, error) {
		first := ctx.executeChildWorkflow(ChildWorkflowOptions{WorkflowType: "first-child"}, "one")
		second := ctx.executeChildWorkflow(ChildWorkflowOptions{WorkflowType: "second-child"}, "two")
		var firstResult, secondResult string
		if err := first.Get(ctx, &firstResult); err != nil {
			return "", err
		}
		if err := second.Get(ctx, &secondResult); err != nil {
			return "", err
		}
		return firstResult + "|" + secondResult, nil
	})

	commands := successfulCommands(t, worker.handleActivation(
		initializeActivation("run-1", "parallel-children", nil),
	))
	if len(commands) != 2 {
		t.Fatalf("initial completion has %d commands, want 2 parallel children", len(commands))
	}
	first := commands[0].GetStartChildWorkflowExecution()
	second := commands[1].GetStartChildWorkflowExecution()
	if first == nil || second == nil || first.Seq != 1 || second.Seq != 2 ||
		first.WorkflowType != "first-child" || second.WorkflowType != "second-child" {
		t.Fatalf("parallel child commands are not deterministic: %v", commands)
	}
	for _, child := range []*workflowcommands.StartChildWorkflowExecution{first, second} {
		if child.WorkflowExecutionTimeout != nil || child.WorkflowRunTimeout != nil ||
			child.WorkflowTaskTimeout != nil {
			t.Fatalf("unset child timeouts must remain omitted: %v", child)
		}
		if child.WorkflowIdReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE {
			t.Fatalf("default child workflow ID reuse policy = %v, want ALLOW_DUPLICATE", child.WorkflowIdReusePolicy)
		}
	}

	started := worker.handleActivation(activation(
		"run-1",
		childWorkflowStartedJob(2, "second-run"),
		childWorkflowStartedJob(1, "first-run"),
	))
	if commands := successfulCommands(t, started); len(commands) != 0 {
		t.Fatalf("parallel child starts emitted %d commands, want 0", len(commands))
	}

	firstPayload, _ := worker.payloadConverter.ToPayload("first-result")
	secondPayload, _ := worker.payloadConverter.ToPayload("second-result")
	completion := worker.handleActivation(activation(
		"run-1",
		childWorkflowCompletedJob(2, secondPayload),
		childWorkflowCompletedJob(1, firstPayload),
	))
	complete := onlyCommand(t, completion).GetCompleteWorkflowExecution()
	if complete == nil {
		t.Fatal("parallel child workflow parent did not complete")
	}
	var result string
	if err := worker.payloadConverter.FromPayload(complete.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result != "first-result|second-result" {
		t.Fatalf("parallel child result = %q", result)
	}
}

func TestParallelChildWorkflowsReplayRegeneratesExactCommandStream(t *testing.T) {
	register := func(worker *Worker) {
		worker.Register("parallel-children", func(ctx *Context) error {
			first := ctx.executeChildWorkflow(ChildWorkflowOptions{WorkflowType: "first"})
			second := ctx.executeChildWorkflow(ChildWorkflowOptions{WorkflowType: "second"})
			if err := first.Get(ctx, nil); err != nil {
				return err
			}
			return second.Get(ctx, nil)
		})
	}

	live := newTestWorker()
	register(live)
	want := append([]*workflowcommands.WorkflowCommand{}, successfulCommands(t, live.handleActivation(
		initializeActivation("run-1", "parallel-children", nil),
	))...)
	successfulCommands(t, live.handleActivation(activation(
		"run-1",
		childWorkflowStartedJob(1, "first-run"),
		childWorkflowStartedJob(2, "second-run"),
	)))
	want = append(want, successfulCommands(t, live.handleActivation(activation(
		"run-1",
		childWorkflowCompletedJob(2, nil),
		childWorkflowCompletedJob(1, nil),
	)))...)

	restarted := newTestWorker()
	register(restarted)
	replay := initializeActivation("run-1", "parallel-children", nil)
	replay.IsReplaying = true
	replay.Jobs = append(
		replay.Jobs,
		childWorkflowStartedJob(1, "first-run"),
		childWorkflowStartedJob(2, "second-run"),
		childWorkflowCompletedJob(2, nil),
		childWorkflowCompletedJob(1, nil),
	)
	got := successfulCommands(t, restarted.handleActivation(replay))
	if len(got) != len(want) {
		t.Fatalf("parallel child replay produced %d commands, want %d", len(got), len(want))
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("parallel child replay command %d differs:\n got %v\nwant %v", i, got[i], want[i])
		}
	}
}

func TestChildStartFutureCanDriveWorkBeforeChildCompletion(t *testing.T) {
	worker := newTestWorker()
	worker.Register("child-start-future", func(ctx *Context) error {
		child := ctx.executeChildWorkflow(ChildWorkflowOptions{WorkflowType: "child"})
		var execution WorkflowExecution
		if err := child.GetChildWorkflowExecution().Get(ctx, &execution); err != nil {
			return err
		}
		if execution.ID != "run-1_1" || execution.RunID != "child-run" {
			return fmt.Errorf("unexpected child execution: %+v", execution)
		}
		activity := ctx.executeActivity(ActivityOptions{
			ActivityType:        "after-child-start",
			StartToCloseTimeout: time.Minute,
		})
		if err := activity.Get(ctx, nil); err != nil {
			return err
		}
		return child.Get(ctx, nil)
	})

	worker.handleActivation(initializeActivation("run-1", "child-start-future", nil))
	afterStart := worker.handleActivation(activation("run-1", childWorkflowStartedJob(1, "child-run")))
	schedule := onlyCommand(t, afterStart).GetScheduleActivity()
	if schedule == nil || schedule.Seq != 2 || schedule.ActivityType != "after-child-start" {
		t.Fatalf("child start future did not schedule follow-up work: %v", schedule)
	}
	activityResult, _ := worker.payloadConverter.ToPayload("ignored")
	blocked := worker.handleActivation(activation("run-1", resolveActivityJob(2, activityResult)))
	if commands := successfulCommands(t, blocked); len(commands) != 0 {
		t.Fatalf("parent emitted %d commands while awaiting child completion", len(commands))
	}
	completed := worker.handleActivation(activation("run-1", childWorkflowCompletedJob(1, nil)))
	if onlyCommand(t, completed).GetCompleteWorkflowExecution() == nil {
		t.Fatal("parent did not complete after child completion")
	}
}

func TestChildFutureReadinessHasSeparateStartAndCompletionPhases(t *testing.T) {
	worker := newTestWorker()
	var child ChildWorkflowFuture
	worker.Register("child-readiness", func(ctx *Context) error {
		child = ctx.executeChildWorkflow(ChildWorkflowOptions{WorkflowType: "child"})
		return child.Get(ctx, nil)
	})

	worker.handleActivation(initializeActivation("run-1", "child-readiness", nil))
	if child == nil {
		t.Fatal("workflow did not create a child future")
	}
	if child.IsReady() || child.GetChildWorkflowExecution().IsReady() {
		t.Fatalf("child futures were ready before start: result=%v start=%v", child.IsReady(), child.GetChildWorkflowExecution().IsReady())
	}
	worker.handleActivation(activation("run-1", childWorkflowStartedJob(1, "child-run")))
	if child.IsReady() || !child.GetChildWorkflowExecution().IsReady() {
		t.Fatalf("child readiness after start: result=%v start=%v", child.IsReady(), child.GetChildWorkflowExecution().IsReady())
	}
	worker.handleActivation(activation("run-1", childWorkflowCompletedJob(1, nil)))
	if !child.IsReady() || !child.GetChildWorkflowExecution().IsReady() {
		t.Fatalf("child readiness after completion: result=%v start=%v", child.IsReady(), child.GetChildWorkflowExecution().IsReady())
	}
}

func TestFutureChainResolvesWaitingCoroutine(t *testing.T) {
	worker := newTestWorker()
	worker.Register("future-chain", func(ctx *Context) (string, error) {
		source, sourceSettable := NewFuture(ctx)
		chained, chainedSettable := NewFuture(ctx)
		chainedSettable.Chain(source)
		Go(ctx, func(*Context) {
			sourceSettable.SetValue("chained-result")
		})
		var result string
		if err := chained.Get(ctx, &result); err != nil {
			return "", err
		}
		return result, nil
	})

	completion := worker.handleActivation(initializeActivation("run-1", "future-chain", nil))
	complete := onlyCommand(t, completion).GetCompleteWorkflowExecution()
	if complete == nil {
		t.Fatal("future chain workflow did not complete")
	}
	var result string
	if err := worker.payloadConverter.FromPayload(complete.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result != "chained-result" {
		t.Fatalf("future chain result = %q, want chained-result", result)
	}
}

func TestChildWorkflowReplayRegeneratesTheSameCommands(t *testing.T) {
	register := func(worker *Worker) {
		worker.Register("parent", func(ctx *Context) (string, error) {
			var result string
			err := ctx.executeChildWorkflow(ChildWorkflowOptions{WorkflowType: "child"}).Get(ctx, &result)
			return result, err
		})
	}
	output, err := converter.GetDefaultPayloadConverter().ToPayload("done")
	if err != nil {
		t.Fatal(err)
	}

	live := newTestWorker()
	register(live)
	first := successfulCommands(t, live.handleActivation(initializeActivation("run-1", "parent", nil)))
	middle := successfulCommands(t, live.handleActivation(activation("run-1", childWorkflowStartedJob(1, "child-run"))))
	last := successfulCommands(t, live.handleActivation(activation("run-1", childWorkflowCompletedJob(1, output))))
	want := append(append(append([]*workflowcommands.WorkflowCommand{}, first...), middle...), last...)

	restarted := newTestWorker()
	register(restarted)
	replay := initializeActivation("run-1", "parent", nil)
	replay.IsReplaying = true
	replay.Jobs = append(replay.Jobs, childWorkflowStartedJob(1, "child-run"), childWorkflowCompletedJob(1, output))
	got := successfulCommands(t, restarted.handleActivation(replay))
	if len(got) != len(want) {
		t.Fatalf("replay produced %d commands, want %d", len(got), len(want))
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("replay command %d differs:\n got %v\nwant %v", i, got[i], want[i])
		}
	}
}

func TestChildWorkflowForwardsExplicitRouting(t *testing.T) {
	worker := newTestWorker()
	worker.Register("parent", func(ctx *Context) error {
		return ctx.executeChildWorkflow(ChildWorkflowOptions{
			Namespace:    "other-namespace",
			WorkflowID:   "explicit-child-id",
			WorkflowType: "child",
			TaskQueue:    "other-queue",
		}).Get(ctx, nil)
	})

	start := onlyCommand(t, worker.handleActivation(initializeActivation("run-1", "parent", nil))).GetStartChildWorkflowExecution()
	if start == nil {
		t.Fatal("workflow did not emit StartChildWorkflowExecution")
	}
	if start.Namespace != "other-namespace" || start.WorkflowId != "explicit-child-id" || start.TaskQueue != "other-queue" {
		t.Fatalf("explicit child routing was not preserved: %v", start)
	}
}

func TestChildWorkflowFailuresFailTheParentWorkflow(t *testing.T) {
	testCases := []struct {
		name       string
		resolution func() *workflowactivation.WorkflowActivationJob
		want       string
	}{
		{
			name: "start failed",
			resolution: func() *workflowactivation.WorkflowActivationJob {
				return childWorkflowStartFailedJob(1, "child-id", "child")
			},
			want: "failed to start",
		},
		{
			name: "start cancelled",
			resolution: func() *workflowactivation.WorkflowActivationJob {
				return childWorkflowStartCancelledJob(1, "cancelled before start")
			},
			want: "start cancelled",
		},
		{
			name: "execution failed",
			resolution: func() *workflowactivation.WorkflowActivationJob {
				return childWorkflowFailedJob(1, "child failed")
			},
			want: "child workflow failed: child failed",
		},
		{
			name: "execution cancelled",
			resolution: func() *workflowactivation.WorkflowActivationJob {
				return childWorkflowCancelledJob(1, "child cancelled")
			},
			want: "child workflow cancelled: child cancelled",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			worker := newTestWorker()
			worker.Register("parent", func(ctx *Context) error {
				return ctx.executeChildWorkflow(ChildWorkflowOptions{WorkflowType: "child"}).Get(ctx, nil)
			})
			worker.handleActivation(initializeActivation("run-1", "parent", nil))
			if strings.HasPrefix(testCase.name, "execution") {
				started := worker.handleActivation(activation("run-1", childWorkflowStartedJob(1, "child-run")))
				if commands := successfulCommands(t, started); len(commands) != 0 {
					t.Fatalf("child start resolution emitted %d commands, want 0", len(commands))
				}
			}

			completion := worker.handleActivation(activation("run-1", testCase.resolution()))
			failed := onlyCommand(t, completion).GetFailWorkflowExecution()
			if failed == nil || !strings.Contains(failureMessage(failed.Failure), testCase.want) {
				t.Fatalf("parent failure = %v, want message containing %q", failed, testCase.want)
			}
		})
	}
}

func TestExecuteChildWorkflowValidatesOptions(t *testing.T) {
	testCases := []struct {
		name    string
		options ChildWorkflowOptions
		result  any
		want    string
	}{
		{name: "missing type", want: "child workflow type is required"},
		{
			name:    "negative execution timeout",
			options: ChildWorkflowOptions{WorkflowType: "child", WorkflowExecutionTimeout: -time.Second},
			want:    "execution timeout cannot be negative",
		},
		{
			name:    "negative run timeout",
			options: ChildWorkflowOptions{WorkflowType: "child", WorkflowRunTimeout: -time.Second},
			want:    "run timeout cannot be negative",
		},
		{
			name:    "negative task timeout",
			options: ChildWorkflowOptions{WorkflowType: "child", WorkflowTaskTimeout: -time.Second},
			want:    "task timeout cannot be negative",
		},
		{
			name:    "non-pointer result",
			options: ChildWorkflowOptions{WorkflowType: "child"},
			result:  "result",
			want:    "result must be a non-nil pointer",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			worker := newTestWorker()
			worker.Register("parent", func(ctx *Context) error {
				return ctx.executeChildWorkflow(testCase.options).Get(ctx, testCase.result)
			})
			completion := worker.handleActivation(initializeActivation("run-1", "parent", nil))
			var failed *workflowcommands.FailWorkflowExecution
			for _, command := range successfulCommands(t, completion) {
				if command.GetFailWorkflowExecution() != nil {
					failed = command.GetFailWorkflowExecution()
				}
			}
			if failed == nil || !strings.Contains(failureMessage(failed.Failure), testCase.want) {
				t.Fatalf("parent failure = %v, want message containing %q", failed, testCase.want)
			}
		})
	}
}

func TestChildWorkflowRejectsMismatchedResolution(t *testing.T) {
	worker := newTestWorker()
	worker.Register("parent", func(ctx *Context) error {
		return ctx.executeChildWorkflow(ChildWorkflowOptions{WorkflowType: "child"}).Get(ctx, nil)
	})
	worker.handleActivation(initializeActivation("run-1", "parent", nil))

	completion := worker.handleActivation(activation("run-1", childWorkflowStartedJob(2, "child-run")))
	failed := completion.GetFailed()
	if failed == nil || !strings.Contains(failureMessage(failed.Failure), "does not match an outstanding operation") {
		t.Fatalf("activation failure = %v, want child start operation mismatch", failed)
	}
}

func TestActivationWithoutCachedStateFails(t *testing.T) {
	worker := newTestWorker()
	completion := worker.handleActivation(activation("missing", fireTimerJob(1)))
	failed := completion.GetFailed()
	if failed == nil || failed.Failure == nil {
		t.Fatalf("completion status is %T, want failed", completion.Status)
	}
}

func TestNewNormalizesWorkflowOptions(t *testing.T) {
	oldNewWorkflowCoreWorker := newWorkflowCoreWorker
	defer func() { newWorkflowCoreWorker = oldNewWorkflowCoreWorker }()

	var gotAddress string
	var gotNamespace string
	var gotTaskQueue string
	var gotIdentity string
	var gotOptions workflowCoreOptions
	newWorkflowCoreWorker = func(
		address, namespace, taskQueue, identity string,
		options workflowCoreOptions,
	) (coreWorker, error) {
		gotAddress = address
		gotNamespace = namespace
		gotTaskQueue = taskQueue
		gotIdentity = identity
		gotOptions = options
		return newFakeWorkflowCore(), nil
	}

	tlsConfig := &tls.Config{ServerName: "example.tmprl.cloud"}
	worker, err := New(Options{TaskQueue: "queue", TLS: tlsConfig, APIKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if gotAddress != "localhost:7233" || gotNamespace != "default" || gotTaskQueue != "queue" || gotIdentity != "go-sdk-on-wasm-core" {
		t.Fatalf("New forwarded unexpected worker identity: %q %q %q %q", gotAddress, gotNamespace, gotTaskQueue, gotIdentity)
	}
	if gotOptions.maxConcurrentWorkflowTaskExecutions != defaultMaxConcurrentWorkflowTaskExecutions {
		t.Fatalf("normalized workflow executions = %d, want %d", gotOptions.maxConcurrentWorkflowTaskExecutions, defaultMaxConcurrentWorkflowTaskExecutions)
	}
	if gotOptions.maxConcurrentWorkflowTaskPollers != 0 {
		t.Fatalf("normalized workflow pollers = %d, want 0", gotOptions.maxConcurrentWorkflowTaskPollers)
	}
	if gotOptions.maxCachedWorkflows != defaultMaxCachedWorkflows {
		t.Fatalf("normalized cached workflows = %d, want %d", gotOptions.maxCachedWorkflows, defaultMaxCachedWorkflows)
	}
	if gotOptions.tls != tlsConfig || gotOptions.apiKey != "secret" {
		t.Fatalf("normalized TLS/API key = %p/%q, want %p/%q", gotOptions.tls, gotOptions.apiKey, tlsConfig, "secret")
	}
	if worker.maxConcurrentWorkflowTaskExecutions != defaultMaxConcurrentWorkflowTaskExecutions {
		t.Fatalf("worker concurrency limit = %d, want %d", worker.maxConcurrentWorkflowTaskExecutions, defaultMaxConcurrentWorkflowTaskExecutions)
	}
}

func TestNewWithCoreUsesProvidedCore(t *testing.T) {
	oldNewWorkflowCoreWorker := newWorkflowCoreWorker
	defer func() { newWorkflowCoreWorker = oldNewWorkflowCoreWorker }()
	newWorkflowCoreWorker = func(string, string, string, string, workflowCoreOptions) (coreWorker, error) {
		t.Fatal("NewWithCore reached core construction")
		return nil, nil
	}

	core := newFakeWorkflowCore()
	worker, err := NewWithCore(Options{TaskQueue: "queue"}, core)
	if err != nil {
		t.Fatal(err)
	}
	if worker.core != core {
		t.Fatal("NewWithCore did not reuse the provided core worker")
	}
	if worker.maxConcurrentWorkflowTaskExecutions != defaultMaxConcurrentWorkflowTaskExecutions {
		t.Fatalf("worker concurrency limit = %d, want %d", worker.maxConcurrentWorkflowTaskExecutions, defaultMaxConcurrentWorkflowTaskExecutions)
	}
}

func TestNewRejectsInvalidWorkflowOptions(t *testing.T) {
	oldNewWorkflowCoreWorker := newWorkflowCoreWorker
	defer func() { newWorkflowCoreWorker = oldNewWorkflowCoreWorker }()
	newWorkflowCoreWorker = func(string, string, string, string, workflowCoreOptions) (coreWorker, error) {
		t.Fatal("New reached core construction for invalid options")
		return nil, nil
	}

	testCases := []struct {
		name    string
		options Options
		want    string
	}{
		{
			name:    "missing task queue",
			options: Options{},
			want:    "workflow task queue is required",
		},
		{
			name:    "negative executions",
			options: Options{TaskQueue: "queue", MaxConcurrentWorkflowTaskExecutionSize: -1},
			want:    "maximum concurrent workflow task executions must be between 1 and 2147483647",
		},
		{
			name:    "execution overflow",
			options: Options{TaskQueue: "queue", MaxConcurrentWorkflowTaskExecutionSize: math.MaxInt32 + 1},
			want:    "maximum concurrent workflow task executions must be between 1 and 2147483647",
		},
		{
			name:    "cache requires two executions",
			options: Options{TaskQueue: "queue", MaxConcurrentWorkflowTaskExecutionSize: 1, MaxCachedWorkflows: 1},
			want:    "maximum concurrent workflow task executions must be at least 2 when cached workflows are enabled",
		},
		{
			name:    "negative pollers",
			options: Options{TaskQueue: "queue", MaxConcurrentWorkflowTaskPollers: -1},
			want:    "maximum concurrent workflow task pollers must be between 0 and 2147483647",
		},
		{
			name:    "poller overflow",
			options: Options{TaskQueue: "queue", MaxConcurrentWorkflowTaskPollers: math.MaxInt32 + 1},
			want:    "maximum concurrent workflow task pollers must be between 0 and 2147483647",
		},
		{
			name:    "cache requires two pollers when fixed",
			options: Options{TaskQueue: "queue", MaxConcurrentWorkflowTaskPollers: 1, MaxCachedWorkflows: 1},
			want:    "maximum concurrent workflow task pollers must be at least 2 when cached workflows are enabled",
		},
		{
			name:    "negative cache",
			options: Options{TaskQueue: "queue", MaxCachedWorkflows: -1},
			want:    "maximum cached workflows must be between 1 and 2147483647",
		},
		{
			name:    "cache overflow",
			options: Options{TaskQueue: "queue", MaxCachedWorkflows: math.MaxInt32 + 1},
			want:    "maximum cached workflows must be between 1 and 2147483647",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := New(testCase.options)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("New error = %v, want substring %q", err, testCase.want)
			}
		})
	}
}

func TestRunExecutesWorkflowRunsInParallel(t *testing.T) {
	core := newFakeWorkflowCore()
	worker := runTestWorker(core, 4)
	var startedCount atomic.Int32
	allStarted := make(chan struct{})
	release := make(chan struct{})
	worker.Register("parallel", func(*Context) error {
		if startedCount.Add(1) == 4 {
			close(allStarted)
		}
		<-release
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- worker.Run(ctx) }()

	for i := 0; i < 4; i++ {
		core.sendActivation(t, initializeActivation(fmt.Sprintf("run-%d", i), "parallel", nil))
	}
	waitForSignal(t, allStarted, "all workflow runs to start")
	close(release)

	for i := 0; i < 4; i++ {
		completion := core.nextCompletion(t)
		if onlyCommand(t, completion).GetCompleteWorkflowExecution() == nil {
			t.Fatalf("completion command is %T, want CompleteWorkflowExecution", onlyCommand(t, completion).Variant)
		}
	}
	cancel()
	waitForRun(t, runResult)
	if got := core.shutdownCalls.Load(); got != 1 {
		t.Fatalf("shutdown calls = %d, want 1", got)
	}
}

func TestRunDecodesActivationBeforePayloadConversionAndEncodesCompletionAfter(t *testing.T) {
	core := newFakeWorkflowCore()
	worker := runTestWorker(core, 2)
	codec := workflowBoundaryCodec{}
	worker.payloadCodec = codec
	worker.Register("greet", func(_ *Context, name string) (string, error) {
		return "Hello, " + name, nil
	})

	input, err := worker.payloadConverter.ToPayload("Temporal")
	if err != nil {
		t.Fatal(err)
	}
	encodedInput, err := codec.Encode([]*commonpb.Payload{input})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- worker.Run(ctx) }()
	core.sendActivation(t, initializeActivation("run-codec", "greet", encodedInput))

	completion := core.nextCompletion(t)
	result := onlyCommand(t, completion).GetCompleteWorkflowExecution().Result
	if string(result.Metadata["codec"]) != "boundary" {
		t.Fatal("workflow result was not encoded at the Core boundary")
	}
	decodedResult, err := codec.Decode([]*commonpb.Payload{result})
	if err != nil {
		t.Fatal(err)
	}
	var greeting string
	if err := worker.payloadConverter.FromPayload(decodedResult[0], &greeting); err != nil {
		t.Fatal(err)
	}
	if greeting != "Hello, Temporal" {
		t.Fatalf("workflow result = %q", greeting)
	}

	cancel()
	waitForRun(t, runResult)
}

func TestRunTurnsPayloadCodecErrorsIntoWorkflowTaskFailures(t *testing.T) {
	tests := []struct {
		name   string
		codec  converter.PayloadCodec
		prefix string
	}{
		{
			name:   "decode",
			codec:  failingWorkflowCodec{decodeErr: errors.New("decode unavailable")},
			prefix: "decode workflow activation payloads:",
		},
		{
			name:   "encode",
			codec:  failingWorkflowCodec{encodeErr: errors.New("encode unavailable")},
			prefix: "encode workflow activation completion payloads:",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core := newFakeWorkflowCore()
			worker := runTestWorker(core, 2)
			worker.payloadCodec = test.codec
			worker.Register("result", func(*Context) (string, error) { return "done", nil })

			input, err := worker.payloadConverter.ToPayload("input")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			runResult := make(chan error, 1)
			go func() { runResult <- worker.Run(ctx) }()
			core.sendActivation(t, initializeActivation("run-codec-error", "result", []*commonpb.Payload{input}))

			completion := core.nextCompletion(t)
			failed := completion.GetFailed()
			if failed == nil || failed.Failure == nil ||
				!strings.Contains(failed.Failure.Message, test.prefix) ||
				!strings.Contains(failed.Failure.Message, test.name+" unavailable") {
				t.Fatalf("codec failure completion = %v, want %s context", completion, test.name)
			}

			cancel()
			waitForRun(t, runResult)
		})
	}
}

func TestRunSerializesSameWorkflowRunActivations(t *testing.T) {
	core := newFakeWorkflowCore()
	worker := runTestWorker(core, 2)
	worker.Register("timer", func(ctx *Context) error {
		return ctx.sleep(time.Second)
	})

	oldHook := workflowRunLockHook
	defer func() { workflowRunLockHook = oldHook }()
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var enterCount atomic.Int32
	workflowRunLockHook = func(runID string, phase string) {
		if runID != "run-1" || phase != "enter" {
			return
		}
		if enterCount.Add(1) == 1 {
			close(firstEntered)
			<-releaseFirst
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- worker.Run(ctx) }()

	core.sendActivation(t, initializeActivation("run-1", "timer", nil))
	core.sendActivation(t, activation("run-1", fireTimerJob(1)))
	waitForSignal(t, firstEntered, "the first activation to enter the run lock")
	select {
	case completion := <-core.completions:
		t.Fatalf("received completion for %q before releasing the first run lock", completion.RunId)
	case <-time.After(150 * time.Millisecond):
	}
	if got := enterCount.Load(); got != 1 {
		t.Fatalf("same-run activations entered %d times before release, want 1", got)
	}
	close(releaseFirst)

	first := core.nextCompletion(t)
	if onlyCommand(t, first).GetStartTimer() == nil {
		t.Fatalf("first completion command is %T, want StartTimer", onlyCommand(t, first).Variant)
	}
	second := core.nextCompletion(t)
	if onlyCommand(t, second).GetCompleteWorkflowExecution() == nil {
		t.Fatalf("second completion command is %T, want CompleteWorkflowExecution", onlyCommand(t, second).Variant)
	}
	if got := enterCount.Load(); got != 2 {
		t.Fatalf("same-run activations entered %d times, want 2", got)
	}
	cancel()
	waitForRun(t, runResult)
}

func TestRunWaitsForCompletionSubmissionBeforeReleasingSameRun(t *testing.T) {
	core := newFakeWorkflowCore()
	releaseFirstCompletion := make(chan struct{})
	firstSubmissionStarted := make(chan struct{}, 1)
	core.onComplete = func(completion *workflowcompletion.WorkflowActivationCompletion) error {
		if completion.RunId != "run-1" {
			return nil
		}
		if onlyCommand(t, completion).GetStartTimer() != nil {
			select {
			case firstSubmissionStarted <- struct{}{}:
			default:
			}
			<-releaseFirstCompletion
		}
		return nil
	}

	worker := runTestWorker(core, 2)
	worker.Register("timer", func(ctx *Context) error {
		return ctx.sleep(time.Second)
	})

	var enterCount atomic.Int32
	oldHook := workflowRunLockHook
	defer func() { workflowRunLockHook = oldHook }()
	workflowRunLockHook = func(runID string, phase string) {
		if runID == "run-1" && phase == "enter" {
			enterCount.Add(1)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- worker.Run(ctx) }()

	core.sendActivation(t, initializeActivation("run-1", "timer", nil))
	waitForSignal(t, firstSubmissionStarted, "the first completion submission to start")
	firstAttempt := core.nextAttempt(t)
	if firstAttempt.RunId != "run-1" || onlyCommand(t, firstAttempt).GetStartTimer() == nil {
		t.Fatalf("first completion attempt = %#v, want run-1 StartTimer", firstAttempt)
	}
	core.sendActivation(t, activation("run-1", fireTimerJob(1)))

	select {
	case completion := <-core.attempts:
		t.Fatalf("second completion attempt for %q started before the first submission finished", completion.RunId)
	case <-time.After(150 * time.Millisecond):
	}
	if got := enterCount.Load(); got != 1 {
		t.Fatalf("same-run activations entered %d times before submission release, want 1", got)
	}

	close(releaseFirstCompletion)
	first := core.nextCompletion(t)
	if onlyCommand(t, first).GetStartTimer() == nil {
		t.Fatalf("first completion command is %T, want StartTimer", onlyCommand(t, first).Variant)
	}
	second := core.nextCompletion(t)
	if onlyCommand(t, second).GetCompleteWorkflowExecution() == nil {
		t.Fatalf("second completion command is %T, want CompleteWorkflowExecution", onlyCommand(t, second).Variant)
	}
	if got := enterCount.Load(); got != 2 {
		t.Fatalf("same-run activations entered %d times, want 2", got)
	}
	cancel()
	waitForRun(t, runResult)
}

func TestRunSubmitsWorkflowCompletionsConcurrently(t *testing.T) {
	core := newFakeWorkflowCore()
	started := make(chan string, 2)
	releaseRun1 := make(chan struct{})
	releaseRun2 := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	core.onComplete = func(completion *workflowcompletion.WorkflowActivationCompletion) error {
		current := active.Add(1)
		for {
			previous := maxActive.Load()
			if current <= previous || maxActive.CompareAndSwap(previous, current) {
				break
			}
		}
		started <- completion.RunId
		defer active.Add(-1)
		if completion.RunId == "run-1" {
			<-releaseRun1
			return nil
		}
		<-releaseRun2
		return nil
	}

	worker := runTestWorker(core, 2)
	worker.Register("complete", func(*Context) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- worker.Run(ctx) }()

	core.sendActivation(t, initializeActivation("run-1", "complete", nil))
	core.sendActivation(t, initializeActivation("run-2", "complete", nil))
	startedRuns := []string{waitForString(t, started, "first completion to start"), waitForString(t, started, "second completion to start")}
	if !sameSet(startedRuns, []string{"run-1", "run-2"}) {
		t.Fatalf("started completions = %v, want both run-1 and run-2", startedRuns)
	}
	if got := maxActive.Load(); got != 2 {
		t.Fatalf("maximum concurrent completions = %d, want 2", got)
	}
	close(releaseRun2)
	if completion := core.nextCompletion(t); completion.RunId != "run-2" {
		t.Fatalf("first finished completion = %q, want run-2", completion.RunId)
	}
	close(releaseRun1)
	if completion := core.nextCompletion(t); completion.RunId != "run-1" {
		t.Fatalf("second finished completion = %q, want run-1", completion.RunId)
	}
	cancel()
	waitForRun(t, runResult)
}

func TestRunDrainsAcceptedActivationsAfterCompletionError(t *testing.T) {
	core := newFakeWorkflowCore()
	allowFailure := make(chan struct{})
	run2Attempted := make(chan struct{})
	core.onComplete = func(completion *workflowcompletion.WorkflowActivationCompletion) error {
		if completion.RunId == "run-1" {
			<-allowFailure
			return errors.New("expected completion failure")
		}
		close(run2Attempted)
		return nil
	}

	worker := runTestWorker(core, 2)
	worker.Register("complete", func(*Context) error { return nil })

	runResult := make(chan error, 1)
	go func() { runResult <- worker.Run(context.Background()) }()

	core.sendActivation(t, initializeActivation("run-1", "complete", nil))
	core.sendActivation(t, initializeActivation("run-2", "complete", nil))
	waitForSignal(t, run2Attempted, "the second accepted activation to attempt completion")
	close(allowFailure)

	err := waitForRunError(t, runResult)
	if !strings.Contains(err.Error(), "expected completion failure") {
		t.Fatalf("Run error = %v, want completion failure", err)
	}
	attemptedRuns := []string{core.nextAttempt(t).RunId, core.nextAttempt(t).RunId}
	if !sameSet(attemptedRuns, []string{"run-1", "run-2"}) {
		t.Fatalf("completion attempts = %v, want run-1 and run-2", attemptedRuns)
	}
	if got := core.shutdownCalls.Load(); got != 1 {
		t.Fatalf("shutdown calls = %d, want 1", got)
	}
}

func TestRunCancellationDrainsAcceptedActivations(t *testing.T) {
	core := newFakeWorkflowCore()
	worker := runTestWorker(core, 2)
	var startedCount atomic.Int32
	allStarted := make(chan struct{})
	release := make(chan struct{})
	worker.Register("parallel", func(*Context) error {
		if startedCount.Add(1) == 2 {
			close(allStarted)
		}
		<-release
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- worker.Run(ctx) }()

	core.sendActivation(t, initializeActivation("run-1", "parallel", nil))
	core.sendActivation(t, initializeActivation("run-2", "parallel", nil))
	waitForSignal(t, allStarted, "both accepted workflow runs to start")
	cancel()
	close(release)

	for i := 0; i < 2; i++ {
		completion := core.nextCompletion(t)
		if onlyCommand(t, completion).GetCompleteWorkflowExecution() == nil {
			t.Fatalf("completion command is %T, want CompleteWorkflowExecution", onlyCommand(t, completion).Variant)
		}
	}
	waitForRun(t, runResult)
}

func newTestWorker() *Worker {
	return &Worker{
		payloadConverter:                    converter.GetDefaultPayloadConverter(),
		namespace:                           "test-namespace",
		taskQueue:                           "test-queue",
		handlers:                            make(map[string]*registeredWorkflow),
		maxConcurrentWorkflowTaskExecutions: defaultMaxConcurrentWorkflowTaskExecutions,
		runs:                                make(map[string]*workflowExecution),
		runLocks:                            make(map[string]*workflowRunLane),
	}
}

func initializeActivation(runID, workflowType string, arguments []*commonpb.Payload) *workflowactivation.WorkflowActivation {
	return activation(runID, &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_InitializeWorkflow{
			InitializeWorkflow: &workflowactivation.InitializeWorkflow{
				WorkflowType: workflowType,
				Arguments:    arguments,
			},
		},
	})
}

func activation(runID string, jobs ...*workflowactivation.WorkflowActivationJob) *workflowactivation.WorkflowActivation {
	return &workflowactivation.WorkflowActivation{RunId: runID, Jobs: jobs}
}

func fireTimerJob(seq uint32) *workflowactivation.WorkflowActivationJob {
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_FireTimer{
			FireTimer: &workflowactivation.FireTimer{Seq: seq},
		},
	}
}

func resolveActivityJob(seq uint32, result *commonpb.Payload) *workflowactivation.WorkflowActivationJob {
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_ResolveActivity{
			ResolveActivity: &workflowactivation.ResolveActivity{
				Seq: seq,
				Result: &activityresult.ActivityResolution{
					Status: &activityresult.ActivityResolution_Completed{
						Completed: &activityresult.Success{Result: result},
					},
				},
			},
		},
	}
}

func childWorkflowStartedJob(seq uint32, runID string) *workflowactivation.WorkflowActivationJob {
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_ResolveChildWorkflowExecutionStart{
			ResolveChildWorkflowExecutionStart: &workflowactivation.ResolveChildWorkflowExecutionStart{
				Seq: seq,
				Status: &workflowactivation.ResolveChildWorkflowExecutionStart_Succeeded{
					Succeeded: &workflowactivation.ResolveChildWorkflowExecutionStartSuccess{RunId: runID},
				},
			},
		},
	}
}

func childWorkflowStartFailedJob(seq uint32, workflowID, workflowType string) *workflowactivation.WorkflowActivationJob {
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_ResolveChildWorkflowExecutionStart{
			ResolveChildWorkflowExecutionStart: &workflowactivation.ResolveChildWorkflowExecutionStart{
				Seq: seq,
				Status: &workflowactivation.ResolveChildWorkflowExecutionStart_Failed{
					Failed: &workflowactivation.ResolveChildWorkflowExecutionStartFailure{
						WorkflowId:   workflowID,
						WorkflowType: workflowType,
						Cause:        childworkflow.StartChildWorkflowExecutionFailedCause_START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS,
					},
				},
			},
		},
	}
}

func childWorkflowStartCancelledJob(seq uint32, message string) *workflowactivation.WorkflowActivationJob {
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_ResolveChildWorkflowExecutionStart{
			ResolveChildWorkflowExecutionStart: &workflowactivation.ResolveChildWorkflowExecutionStart{
				Seq: seq,
				Status: &workflowactivation.ResolveChildWorkflowExecutionStart_Cancelled{
					Cancelled: &workflowactivation.ResolveChildWorkflowExecutionStartCancelled{
						Failure: &failurepb.Failure{Message: message},
					},
				},
			},
		},
	}
}

func childWorkflowCompletedJob(seq uint32, result *commonpb.Payload) *workflowactivation.WorkflowActivationJob {
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_ResolveChildWorkflowExecution{
			ResolveChildWorkflowExecution: &workflowactivation.ResolveChildWorkflowExecution{
				Seq: seq,
				Result: &childworkflow.ChildWorkflowResult{
					Status: &childworkflow.ChildWorkflowResult_Completed{
						Completed: &childworkflow.Success{Result: result},
					},
				},
			},
		},
	}
}

func childWorkflowFailedJob(seq uint32, message string) *workflowactivation.WorkflowActivationJob {
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_ResolveChildWorkflowExecution{
			ResolveChildWorkflowExecution: &workflowactivation.ResolveChildWorkflowExecution{
				Seq: seq,
				Result: &childworkflow.ChildWorkflowResult{
					Status: &childworkflow.ChildWorkflowResult_Failed{
						Failed: &childworkflow.Failure{Failure: &failurepb.Failure{Message: message}},
					},
				},
			},
		},
	}
}

func childWorkflowCancelledJob(seq uint32, message string) *workflowactivation.WorkflowActivationJob {
	return &workflowactivation.WorkflowActivationJob{
		Variant: &workflowactivation.WorkflowActivationJob_ResolveChildWorkflowExecution{
			ResolveChildWorkflowExecution: &workflowactivation.ResolveChildWorkflowExecution{
				Seq: seq,
				Result: &childworkflow.ChildWorkflowResult{
					Status: &childworkflow.ChildWorkflowResult_Cancelled{
						Cancelled: &childworkflow.Cancellation{Failure: &failurepb.Failure{Message: message}},
					},
				},
			},
		},
	}
}

func successfulCommands(t *testing.T, completion *workflowcompletion.WorkflowActivationCompletion) []*workflowcommands.WorkflowCommand {
	t.Helper()
	successful := completion.GetSuccessful()
	if successful == nil {
		t.Fatalf("completion status is %T, want successful", completion.Status)
	}
	return successful.Commands
}

func onlyCommand(t *testing.T, completion *workflowcompletion.WorkflowActivationCompletion) *workflowcommands.WorkflowCommand {
	t.Helper()
	commands := successfulCommands(t, completion)
	if len(commands) != 1 {
		t.Fatalf("completion has %d commands, want 1", len(commands))
	}
	return commands[0]
}

func payloads(values []*commonpb.Payload) *commonpb.Payloads {
	return &commonpb.Payloads{Payloads: values}
}

type fakeWorkflowCore struct {
	polls         chan workflowPollEvent
	attempts      chan *workflowcompletion.WorkflowActivationCompletion
	completions   chan *workflowcompletion.WorkflowActivationCompletion
	onComplete    func(*workflowcompletion.WorkflowActivationCompletion) error
	shutdownCalls atomic.Int32
}

func newFakeWorkflowCore() *fakeWorkflowCore {
	return &fakeWorkflowCore{
		polls:       make(chan workflowPollEvent, 16),
		attempts:    make(chan *workflowcompletion.WorkflowActivationCompletion, 16),
		completions: make(chan *workflowcompletion.WorkflowActivationCompletion, 16),
	}
}

func (f *fakeWorkflowCore) PollWorkflow(ctx context.Context) ([]byte, error) {
	select {
	case event := <-f.polls:
		if event.err != nil {
			return nil, event.err
		}
		return proto.Marshal(event.activation)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeWorkflowCore) CompleteWorkflow(encoded []byte) error {
	completion := &workflowcompletion.WorkflowActivationCompletion{}
	if err := proto.Unmarshal(encoded, completion); err != nil {
		return err
	}
	f.attempts <- completion
	if f.onComplete != nil {
		if err := f.onComplete(completion); err != nil {
			return err
		}
	}
	f.completions <- completion
	return nil
}

func (f *fakeWorkflowCore) Shutdown() error {
	f.shutdownCalls.Add(1)
	return nil
}

func (f *fakeWorkflowCore) sendActivation(t *testing.T, activation *workflowactivation.WorkflowActivation) {
	t.Helper()
	f.polls <- workflowPollEvent{activation: activation}
}

func (f *fakeWorkflowCore) nextAttempt(t *testing.T) *workflowcompletion.WorkflowActivationCompletion {
	t.Helper()
	select {
	case completion := <-f.attempts:
		return completion
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for workflow completion attempt")
		return nil
	}
}

func (f *fakeWorkflowCore) nextCompletion(t *testing.T) *workflowcompletion.WorkflowActivationCompletion {
	t.Helper()
	select {
	case completion := <-f.completions:
		return completion
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for workflow completion")
		return nil
	}
}

func runTestWorker(core *fakeWorkflowCore, maxConcurrent int) *Worker {
	return &Worker{
		core:                                core,
		payloadConverter:                    converter.GetDefaultPayloadConverter(),
		taskQueue:                           "test-queue",
		handlers:                            make(map[string]*registeredWorkflow),
		maxConcurrentWorkflowTaskExecutions: maxConcurrent,
		runs:                                make(map[string]*workflowExecution),
		runLocks:                            make(map[string]*workflowRunLane),
	}
}

type workflowBoundaryCodec struct{}

func (workflowBoundaryCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for i, payload := range payloads {
		result[i] = proto.Clone(payload).(*commonpb.Payload)
		if result[i].Metadata == nil {
			result[i].Metadata = make(map[string][]byte)
		}
		result[i].Metadata["codec"] = []byte("boundary")
		result[i].Data = append([]byte("coded:"), result[i].Data...)
	}
	return result, nil
}

type failingWorkflowCodec struct {
	encodeErr error
	decodeErr error
}

func (c failingWorkflowCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return payloads, c.encodeErr
}

func (c failingWorkflowCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return payloads, c.decodeErr
}

func (workflowBoundaryCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for i, payload := range payloads {
		if string(payload.Metadata["codec"]) != "boundary" || !strings.HasPrefix(string(payload.Data), "coded:") {
			return nil, errors.New("payload is not boundary encoded")
		}
		result[i] = proto.Clone(payload).(*commonpb.Payload)
		delete(result[i].Metadata, "codec")
		result[i].Data = result[i].Data[len("coded:"):]
	}
	return result, nil
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForString(t *testing.T, signal <-chan string, description string) string {
	t.Helper()
	select {
	case value := <-signal:
		return value
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return ""
	}
}

func waitForRun(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workflow worker did not stop")
	}
}

func waitForRunError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("workflow worker returned nil error")
		}
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("workflow worker did not stop")
		return nil
	}
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	counts := make(map[string]int, len(want))
	for _, value := range want {
		counts[value]++
	}
	for _, value := range got {
		counts[value]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}
