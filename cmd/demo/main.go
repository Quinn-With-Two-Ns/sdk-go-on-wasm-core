package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/temporalio/sdk-go-on-wasm-core/activity"
	"github.com/temporalio/sdk-go-on-wasm-core/worker"
	"github.com/temporalio/sdk-go-on-wasm-core/workflow"
)

const taskQueue = "go-wasm-demo"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	address := envOrDefault("TEMPORAL_ADDRESS", "localhost:7233")
	namespace := envOrDefault("TEMPORAL_NAMESPACE", "default")
	apiKey := os.Getenv("TEMPORAL_API_KEY")
	w, err := worker.New(worker.Options{
		Address:   address,
		Namespace: namespace,
		TaskQueue: taskQueue,
		APIKey:    apiKey,
	})
	if err != nil {
		log.Fatal(err)
	}
	w.RegisterActivity(greet)
	w.RegisterActivity(localGreet)
	w.RegisterWorkflowUntyped("timer-greeting", timerGreeting)
	w.RegisterWorkflowUntyped("local-activity-greeting", localActivityGreeting)
	w.RegisterWorkflowUntyped("child-workflow-greeting", childWorkflowGreeting)
	w.RegisterWorkflow(childGreeting)
	w.RegisterWorkflowUntyped("parallel-greeting", parallelGreeting)
	w.RegisterWorkflowUntyped("signal-greeting", signalGreeting)
	w.RegisterWorkflowUntyped("query-greeting", queryGreeting)
	w.RegisterWorkflowUntyped("update-greeting", updateGreeting)

	log.Printf("worker listening on %q", taskQueue)
	if err := w.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func timerGreeting(ctx *workflow.Context, name string) (string, error) {
	if err := workflow.Sleep(ctx, 5*time.Second); err != nil {
		return "", err
	}
	greeting, err := workflow.ExecuteActivity[string](ctx, greet, workflow.ActivityOptions{
		TaskQueue:           taskQueue,
		StartToCloseTimeout: 10 * time.Second,
	}, name).Get(ctx)
	return greeting, err
}

func localActivityGreeting(ctx *workflow.Context, name string) (string, error) {
	return workflow.ExecuteLocalActivity[string](ctx, localGreet, workflow.LocalActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}, name).Get(ctx)
}

func childWorkflowGreeting(ctx *workflow.Context, name string) (string, error) {
	greeting, err := workflow.ExecuteChildWorkflow[string](ctx, childGreeting, workflow.ChildWorkflowOptions{
		TaskQueue: taskQueue,
	}, name).Get(ctx)
	return greeting, err
}

func childGreeting(_ *workflow.Context, name string) (string, error) {
	return "Hello, " + name + " from a child workflow!", nil
}

func parallelGreeting(ctx *workflow.Context, name string) (string, error) {
	firstDone, firstSettable := workflow.NewFuture[string](ctx)
	secondDone, secondSettable := workflow.NewFuture[string](ctx)
	startBranch := func(branch string, settable workflow.Settable[string]) func(*workflow.Context) {
		return func(ctx *workflow.Context) {
			activityFuture := workflow.ExecuteActivity[string](ctx, greet, workflow.ActivityOptions{
				TaskQueue:           taskQueue,
				StartToCloseTimeout: 10 * time.Second,
			}, name+" "+branch)
			child := workflow.ExecuteChildWorkflow[string](ctx, childGreeting, workflow.ChildWorkflowOptions{
				TaskQueue: taskQueue,
			}, name+" "+branch)
			activityResult, err := activityFuture.Get(ctx)
			if err != nil {
				settable.SetError(err)
				return
			}
			childResult, err := child.Get(ctx)
			if err != nil {
				settable.SetError(err)
				return
			}
			settable.SetValue(activityResult + " / " + childResult)
		}
	}
	workflow.GoNamed(ctx, "first-branch", startBranch("one", firstSettable))
	workflow.GoNamed(ctx, "second-branch", startBranch("two", secondSettable))

	first, err := firstDone.Get(ctx)
	if err != nil {
		return "", err
	}
	second, err := secondDone.Get(ctx)
	if err != nil {
		return "", err
	}
	return first + " | " + second, nil
}

func signalGreeting(ctx *workflow.Context) (string, error) {
	name, err := workflow.GetSignalChannel[string](ctx, "greet").Receive(ctx)
	if err != nil {
		return "", err
	}
	return "Hello, " + name + " from a workflow signal!", nil
}

type greetingStatus struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

func queryGreeting(ctx *workflow.Context) (string, error) {
	status := greetingStatus{State: "waiting for greet signal"}
	if err := workflow.SetQueryHandler(ctx, "status", func() (greetingStatus, error) {
		return status, nil
	}); err != nil {
		return "", err
	}
	name, err := workflow.GetSignalChannel[string](ctx, "greet").Receive(ctx)
	if err != nil {
		return "", err
	}
	status = greetingStatus{Name: name, State: "waiting for finish signal"}
	if _, err := workflow.GetSignalChannel[struct{}](ctx, "finish").Receive(ctx); err != nil {
		return "", err
	}
	return "Hello, " + name + " from a queried workflow!", nil
}

func updateGreeting(ctx *workflow.Context) (string, error) {
	status := greetingStatus{State: "waiting for set-name update"}
	if err := workflow.SetQueryHandler(ctx, "status", func() (greetingStatus, error) {
		return status, nil
	}); err != nil {
		return "", err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, "set-name", func(ctx *workflow.Context, name string) (greetingStatus, error) {
		if err := workflow.Sleep(ctx, time.Second); err != nil {
			return greetingStatus{}, err
		}
		status = greetingStatus{Name: name, State: "name updated"}
		return status, nil
	}, workflow.UpdateHandlerOptions{
		Validator: func(name string) error {
			if strings.TrimSpace(name) == "" {
				return errors.New("name must not be empty")
			}
			return nil
		},
	}); err != nil {
		return "", err
	}
	if _, err := workflow.GetSignalChannel[struct{}](ctx, "finish").Receive(ctx); err != nil {
		return "", err
	}
	return "Hello, " + status.Name + " from a workflow update!", nil
}

func greet(ctx context.Context, name string) (string, error) {
	if err := activity.RecordHeartbeat(ctx, name); err != nil {
		return "", fmt.Errorf("record heartbeat: %w", err)
	}
	return "Hello, " + name + " from Temporal Core compiled through WASM into Go!", nil
}

func localGreet(name string) (string, error) {
	return "Hello, " + name + " from a local activity through Temporal Core!", nil
}
