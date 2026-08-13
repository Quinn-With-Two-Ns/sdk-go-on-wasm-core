package benchmark

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	wasmworker "github.com/temporalio/sdk-go-on-wasm-core/worker"
	wasmworkflow "github.com/temporalio/sdk-go-on-wasm-core/workflow"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/corebridge"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/wasmhost"
)

const (
	benchmarkEnabledEnvironment = "TEMPORAL_BENCHMARK"
	benchmarkAPIKeyEnvironment  = "TEMPORAL_API_KEY"
	benchmarkWorkflowType       = "benchmark-five-parallel-activities"
	benchmarkActivityType       = "benchmark-noop-activity"
	benchmarkInput              = "Temporal"
	workflowExecutionTimeout    = 30 * time.Second
	activityStartToCloseTimeout = 30 * time.Second
	workerKindEnvironment       = "TEMPORAL_BENCHMARK_WORKER_KIND"
	workerTaskQueueEnvironment  = "TEMPORAL_BENCHMARK_TASK_QUEUE"
	warmupExecutions            = 20
	benchmarkWorkflowCapacity   = 1_000
	benchmarkWorkflowPollers    = 2
	benchmarkActivityCapacity   = 1_000
	benchmarkActivityPollers    = 2
	parallelActivityCount       = 5
)

var nextWorkflowID atomic.Uint64

// BenchmarkFiveParallelActivitiesThroughput measures workflows that fan out five activities before
// waiting for all of them. The activities are started before any future is awaited, so every
// workflow exposes five activity tasks to the worker in parallel.
func BenchmarkFiveParallelActivitiesThroughput(b *testing.B) {
	if os.Getenv(benchmarkEnabledEnvironment) != "1" {
		b.Skipf("set %s=1 and configure a Temporal service to run this benchmark", benchmarkEnabledEnvironment)
	}

	address := environmentOrDefault("TEMPORAL_ADDRESS", client.DefaultHostPort)
	namespace := environmentOrDefault("TEMPORAL_NAMESPACE", client.DefaultNamespace)
	apiKey := os.Getenv(benchmarkAPIKeyEnvironment)
	dialContext, cancelDial := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDial()
	temporalClient, err := client.DialContext(dialContext, temporalClientOptions(address, namespace, apiKey))
	if err != nil {
		b.Fatalf("connect to Temporal at %s: %v", address, err)
	}
	b.Cleanup(temporalClient.Close)

	b.Run(goSDKWorkerKind, func(b *testing.B) {
		benchmarkWorkerThroughput(b, temporalClient, address, namespace, apiKey, goSDKWorkerKind)
	})
	b.Run(wasmWorkerKind, func(b *testing.B) {
		benchmarkWorkerThroughput(b, temporalClient, address, namespace, apiKey, wasmWorkerKind)
	})
}

func benchmarkWorkerThroughput(
	b *testing.B,
	temporalClient client.Client,
	address string,
	namespace string,
	apiKey string,
	kind string,
) {
	b.StopTimer()
	unique := fmt.Sprintf("activity-benchmark-%s-%d", kind, time.Now().UnixNano())
	taskQueue := unique + "-queue"
	workerProcess := startWorkerProcess(b, kind, address, namespace, apiKey, taskQueue)
	defer stopWorkerProcess(
		b,
		kind,
		workerProcess,
		int64((warmupExecutions+b.N)*parallelActivityCount),
	)

	concurrency := runtime.GOMAXPROCS(0)
	warmupErr := runFixedParallelism(b.Context(), warmupExecutions, concurrency, func() error {
		workflowID := unique + "-warmup-" + newWorkflowID()
		return executeWorkflow(b.Context(), temporalClient, taskQueue, workflowID)
	})
	if warmupErr != nil {
		b.Fatal(warmupErr)
	}

	b.ResetTimer()
	b.StartTimer()
	benchmarkErr := runFixedParallelism(b.Context(), b.N, concurrency, func() error {
		workflowID := unique + "-measured-" + newWorkflowID()
		return executeWorkflow(b.Context(), temporalClient, taskQueue, workflowID)
	})
	b.StopTimer()
	if benchmarkErr != nil {
		b.Fatal(benchmarkErr)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "workflows/s")
	b.ReportMetric(float64(b.N*parallelActivityCount)/b.Elapsed().Seconds(), "activities/s")
}

func newWorkflowID() string {
	return strconv.FormatUint(nextWorkflowID.Add(1), 10)
}

// TestBenchmarkWorkerProcess is the entry point for benchmark-only child processes. Running each
// implementation out of process gives them equivalent isolation and lets the harness tear down an
// idle WASM Core worker without waiting for its outstanding long poll.
func TestBenchmarkWorkerProcess(t *testing.T) {
	kind := os.Getenv(workerKindEnvironment)
	if kind == "" {
		t.Skip("benchmark worker helper")
	}
	taskQueue := os.Getenv(workerTaskQueueEnvironment)
	if taskQueue == "" {
		t.Fatal("benchmark worker task queue is missing")
	}
	address := environmentOrDefault("TEMPORAL_ADDRESS", client.DefaultHostPort)
	namespace := environmentOrDefault("TEMPORAL_NAMESPACE", client.DefaultNamespace)
	apiKey := os.Getenv(benchmarkAPIKeyEnvironment)
	timingRecorder, err := newBenchmarkTimingRecorderFromEnvironment(kind, taskQueue)
	if err != nil {
		t.Fatal(err)
	}
	eagerCount := &atomic.Int64{}
	var restoreHooks []func()
	restoreHooks = append(restoreHooks, corebridge.SetEagerActivityDispatchHook(func(count int) {
		eagerCount.Add(int64(count))
	}))
	if timingRecorder != nil {
		restoreHooks = append(restoreHooks,
			corebridge.SetTimingHook(func(phase string, elapsed time.Duration) {
				timingRecorder.record("corebridge."+phase, elapsed)
			}),
			wasmhost.SetTimingHook(func(method string, elapsed time.Duration) {
				rpc := method[strings.LastIndex(method, "/")+1:]
				timingRecorder.record("wasmhost.grpc."+sanitizeProfileName(rpc), elapsed)
			}),
		)
	}
	defer func() {
		for i := len(restoreHooks) - 1; i >= 0; i-- {
			restoreHooks[i]()
		}
	}()
	profiler, err := newBenchmarkWorkerProfilerFromEnvironment(kind, taskQueue)
	if err != nil {
		t.Fatal(err)
	}
	if profiler != nil {
		profiler.timingRecorder = timingRecorder
		profiler.eagerCount = eagerCount
	}
	if profiler != nil {
		go profiler.serveFinalizeRequests()
	}

	switch kind {
	case goSDKWorkerKind:
		temporalClient, err := client.Dial(workerTemporalClientOptions(address, namespace, apiKey, timingRecorder, eagerCount))
		if err != nil {
			t.Fatal(err)
		}
		defer temporalClient.Close()
		worker.SetStickyWorkflowCacheSize(benchmarkWorkflowCapacity)
		eagerActivityReservations := parallelActivityCount
		goSDKWorker := worker.New(temporalClient, taskQueue, worker.Options{
			MaxConcurrentWorkflowTaskExecutionSize:      benchmarkWorkflowCapacity,
			MaxConcurrentWorkflowTaskPollers:            benchmarkWorkflowPollers,
			MaxConcurrentActivityExecutionSize:          benchmarkActivityCapacity,
			MaxConcurrentActivityTaskPollers:            benchmarkActivityPollers,
			MaxEagerActivityReservationsPerWorkflowTask: &eagerActivityReservations,
		})
		goSDKWorker.RegisterWorkflowWithOptions(goSDKFiveParallelActivitiesWorkflow, workflow.RegisterOptions{
			Name: benchmarkWorkflowType,
		})
		goSDKWorker.RegisterActivityWithOptions(benchmarkNoopActivity, activity.RegisterOptions{
			Name: benchmarkActivityType,
		})
		if err := goSDKWorker.Run(nil); err != nil {
			t.Fatal(err)
		}
	case wasmWorkerKind:
		wasmWorker, err := wasmworker.New(wasmworker.Options{
			Address:                                address,
			Namespace:                              namespace,
			TaskQueue:                              taskQueue,
			Identity:                               taskQueue + "-worker",
			MaxConcurrentWorkflowTaskExecutionSize: benchmarkWorkflowCapacity,
			MaxConcurrentWorkflowTaskPollers:       benchmarkWorkflowPollers,
			MaxCachedWorkflows:                     benchmarkWorkflowCapacity,
			MaxConcurrentActivityExecutionSize:     benchmarkActivityCapacity,
			MaxConcurrentActivityTaskPollers:       benchmarkActivityPollers,
			MaxEagerActivityReservationsPerWorkflowTask: parallelActivityCount,
			APIKey: apiKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		wasmWorker.RegisterWorkflowUntyped(benchmarkWorkflowType, wasmFiveParallelActivitiesWorkflow)
		wasmWorker.RegisterActivityUntyped(benchmarkActivityType, benchmarkNoopActivity)
		if err := wasmWorker.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown benchmark worker kind %q", kind)
	}
}

func startWorkerProcess(b *testing.B, kind, address, namespace, apiKey, taskQueue string) *benchmarkWorkerProcess {
	b.Helper()
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestBenchmarkWorkerProcess$",
		"-test.cpu="+strconv.Itoa(runtime.GOMAXPROCS(0)),
	)
	command.Env = append(os.Environ(),
		workerKindEnvironment+"="+kind,
		workerTaskQueueEnvironment+"="+taskQueue,
		"TEMPORAL_ADDRESS="+address,
		"TEMPORAL_NAMESPACE="+namespace,
		benchmarkAPIKeyEnvironment+"="+apiKey,
	)
	controller := configureWorkerProfiling(b, command, kind)
	command.Stdout = os.Stderr
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		b.Fatalf("start %s worker process: %v", kind, err)
	}
	if controller != nil {
		controller.closeChildFiles()
	}
	return &benchmarkWorkerProcess{
		command:    command,
		profiler:   controller,
		workerKind: kind,
	}
}

func stopWorkerProcess(b *testing.B, kind string, process *benchmarkWorkerProcess, expectedEagerActivities int64) {
	b.Helper()
	defer process.close()
	if process.profiler != nil {
		profiles, eagerActivities, err := process.profiler.finalize()
		if err != nil {
			b.Fatalf("finalize %s worker profiles: %v", kind, err)
		}
		if eagerActivities != expectedEagerActivities {
			b.Fatalf(
				"%s eagerly dispatched %d activities, want %d",
				kind,
				eagerActivities,
				expectedEagerActivities,
			)
		}
		if len(profiles) > 0 {
			b.Logf("%s worker profiles: %s", kind, strings.Join(profiles, ", "))
		}
	}
	if err := process.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		b.Errorf("stop %s worker process: %v", kind, err)
	}
	var exitErr *exec.ExitError
	if err := process.command.Wait(); err != nil && !errors.As(err, &exitErr) {
		b.Errorf("wait for %s worker process: %v", kind, err)
	}
}

func executeWorkflow(ctx context.Context, temporalClient client.Client, taskQueue, workflowID string) error {
	run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: workflowExecutionTimeout,
	}, benchmarkWorkflowType, benchmarkInput)
	if err != nil {
		return fmt.Errorf("start workflow on %q: %w", taskQueue, err)
	}
	var result string
	if err := run.Get(ctx, &result); err != nil {
		return fmt.Errorf("get workflow result from %q: %w", taskQueue, err)
	}
	if result != benchmarkInput {
		return fmt.Errorf("workflow result from %q is %q, want %q", taskQueue, result, benchmarkInput)
	}
	return nil
}

func goSDKFiveParallelActivitiesWorkflow(ctx workflow.Context, input string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: activityStartToCloseTimeout,
	})
	futures := make([]workflow.Future, parallelActivityCount)
	for index := range futures {
		futures[index] = workflow.ExecuteActivity(ctx, benchmarkActivityType, input)
	}
	for index, future := range futures {
		var result string
		if err := future.Get(ctx, &result); err != nil {
			return "", fmt.Errorf("activity %d: %w", index, err)
		}
		if result != input {
			return "", fmt.Errorf("activity %d result is %q, want %q", index, result, input)
		}
	}
	return input, nil
}

func wasmFiveParallelActivitiesWorkflow(ctx *wasmworkflow.Context, input string) (string, error) {
	options := wasmworkflow.ActivityOptions{StartToCloseTimeout: activityStartToCloseTimeout}
	futures := make([]wasmworkflow.UntypedFuture, parallelActivityCount)
	for index := range futures {
		futures[index] = wasmworkflow.ExecuteActivityUntyped(ctx, benchmarkActivityType, options, input)
	}
	for index, future := range futures {
		var result string
		if err := future.Get(ctx, &result); err != nil {
			return "", fmt.Errorf("activity %d: %w", index, err)
		}
		if result != input {
			return "", fmt.Errorf("activity %d result is %q, want %q", index, result, input)
		}
	}
	return input, nil
}

func benchmarkNoopActivity(input string) (string, error) {
	return input, nil
}

func environmentOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func temporalClientOptions(address, namespace, apiKey string) client.Options {
	options := client.Options{
		HostPort:  address,
		Namespace: namespace,
		Logger:    discardLogger{},
	}
	if apiKey != "" {
		options.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
	}
	return options
}

func workerTemporalClientOptions(
	address,
	namespace,
	apiKey string,
	timingRecorder *benchmarkTimingRecorder,
	eagerCount *atomic.Int64,
) client.Options {
	options := temporalClientOptions(address, namespace, apiKey)
	if timingRecorder != nil {
		options.MetricsHandler = newBenchmarkTimingHandler("go-sdk", timingRecorder)
	}
	options.ConnectionOptions.DialOptions = append(
		options.ConnectionOptions.DialOptions,
		grpc.WithChainUnaryInterceptor(func(
			ctx context.Context,
			method string,
			req, reply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			opts ...grpc.CallOption,
		) error {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil && method == workflowservicepb.WorkflowService_RespondWorkflowTaskCompleted_FullMethodName {
				if response, ok := reply.(*workflowservicepb.RespondWorkflowTaskCompletedResponse); ok {
					eagerCount.Add(int64(len(response.GetActivityTasks())))
					if timingRecorder != nil {
						for range response.GetActivityTasks() {
							timingRecorder.record("go-sdk.eager_activity_dispatch", 0)
						}
					}
				}
			}
			return err
		}),
	)
	return options
}

type discardLogger struct{}

func (discardLogger) Debug(string, ...interface{}) {}
func (discardLogger) Info(string, ...interface{})  {}
func (discardLogger) Warn(string, ...interface{})  {}
func (discardLogger) Error(string, ...interface{}) {}
