package worker

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	internalactivity "github.com/temporalio/sdk-go-on-wasm-core/internal/activityworker"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
	internalworkflow "github.com/temporalio/sdk-go-on-wasm-core/internal/workflowworker"
)

type fakeComponent struct {
	registrations map[string]any
	run           func(context.Context) error
	onShutdown    func() error
	shutdownErr   error
	shutdowns     int
}

func newFakeComponent() *fakeComponent {
	return &fakeComponent{registrations: make(map[string]any)}
}

func (f *fakeComponent) Register(name string, function any) {
	f.registrations[name] = function
}

func (f *fakeComponent) Run(ctx context.Context) error {
	if f.run != nil {
		return f.run(ctx)
	}
	<-ctx.Done()
	return nil
}

func (f *fakeComponent) Shutdown() error {
	f.shutdowns++
	if f.onShutdown != nil {
		return f.onShutdown()
	}
	return f.shutdownErr
}

type fakeCombinedCore struct {
	shutdowns int
}

func (*fakeCombinedCore) PollWorkflow(context.Context) ([]byte, error) { return nil, nil }
func (*fakeCombinedCore) CompleteWorkflow([]byte) error                { return nil }
func (*fakeCombinedCore) PollActivity(context.Context) ([]byte, error) { return nil, nil }
func (*fakeCombinedCore) CompleteActivity([]byte) error                { return nil }
func (*fakeCombinedCore) RecordActivityHeartbeat([]byte) error         { return nil }
func (f *fakeCombinedCore) Shutdown() error {
	f.shutdowns++
	return nil
}

func registeredWorkflowForTest(*internalworkflow.Context) error { return nil }

func registeredActivityForTest() error { return nil }

func stubWorkerFactories(
	t *testing.T,
	workflowFactory func(internalworkflow.Options) (component, error),
	activityFactory func(internalactivity.Options) (component, error),
) {
	t.Helper()
	previousWorkflow := newWorkflowWorker
	previousWorkflowWithCore := newWorkflowWorkerWithCore
	previousActivity := newActivityWorker
	previousActivityWithCore := newActivityWorkerWithCore
	previousCombined := newCombinedCoreWorker
	newWorkflowWorker = workflowFactory
	newWorkflowWorkerWithCore = func(options internalworkflow.Options, _ workflowCore) (component, error) {
		return workflowFactory(options)
	}
	newActivityWorker = activityFactory
	newActivityWorkerWithCore = func(options internalactivity.Options, _ activityCore) (component, error) {
		return activityFactory(options)
	}
	newCombinedCoreWorker = nil
	t.Cleanup(func() {
		newWorkflowWorker = previousWorkflow
		newWorkflowWorkerWithCore = previousWorkflowWithCore
		newActivityWorker = previousActivity
		newActivityWorkerWithCore = previousActivityWithCore
		newCombinedCoreWorker = previousCombined
	})
}

func TestNewCreatesAndRegistersBothWorkers(t *testing.T) {
	workflowComponent := newFakeComponent()
	activityComponent := newFakeComponent()
	var workflowOptions internalworkflow.Options
	var activityOptions internalactivity.Options
	stubWorkerFactories(t,
		func(options internalworkflow.Options) (component, error) {
			workflowOptions = options
			return workflowComponent, nil
		},
		func(options internalactivity.Options) (component, error) {
			activityOptions = options
			return activityComponent, nil
		},
	)

	tlsConfig := &tls.Config{ServerName: "temporal.example"}
	payloadConverter := converter.GetDefaultPayloadConverter()
	payloadCodec := converter.NewNoopPayloadCodec()
	w, err := New(Options{
		Address:                                "temporal.example:7233",
		Namespace:                              "payments",
		TaskQueue:                              "payments-worker",
		Identity:                               "worker-1",
		PayloadConverter:                       payloadConverter,
		PayloadCodec:                           payloadCodec,
		TLS:                                    tlsConfig,
		APIKey:                                 "secret",
		MaxConcurrentActivityExecutionSize:     11,
		MaxConcurrentActivityTaskPollers:       12,
		MaxConcurrentWorkflowTaskExecutionSize: 13,
		MaxConcurrentWorkflowTaskPollers:       14,
		MaxCachedWorkflows:                     15,
	})
	if err != nil {
		t.Fatal(err)
	}
	if workflowOptions.TaskQueue != "payments-worker" || activityOptions.TaskQueue != "payments-worker" {
		t.Fatalf("task queues were not forwarded: workflow=%q activity=%q", workflowOptions.TaskQueue, activityOptions.TaskQueue)
	}
	if workflowOptions.TLS != tlsConfig || activityOptions.TLS != tlsConfig {
		t.Fatal("TLS configuration was not forwarded")
	}
	if workflowOptions.PayloadConverter != payloadConverter || activityOptions.PayloadConverter != payloadConverter {
		t.Fatal("payload converter was not forwarded")
	}
	if workflowOptions.PayloadCodec != payloadCodec || activityOptions.PayloadCodec != payloadCodec {
		t.Fatal("payload codec was not forwarded")
	}
	if workflowOptions.MaxConcurrentWorkflowTaskExecutionSize != 13 || workflowOptions.MaxConcurrentWorkflowTaskPollers != 14 || workflowOptions.MaxCachedWorkflows != 15 {
		t.Fatalf("workflow limits were not forwarded: %+v", workflowOptions)
	}
	if activityOptions.MaxConcurrentActivityExecutionSize != 11 || activityOptions.MaxConcurrentActivityTaskPollers != 12 {
		t.Fatalf("activity limits were not forwarded: %+v", activityOptions)
	}

	workflowFunction := func() error { return nil }
	activityFunction := func() error { return nil }
	w.RegisterWorkflowUntyped("workflow", workflowFunction)
	w.RegisterActivityUntyped("activity", activityFunction)
	w.RegisterWorkflow(registeredWorkflowForTest)
	w.RegisterActivity(registeredActivityForTest)
	if workflowComponent.registrations["workflow"] == nil || activityComponent.registrations["activity"] == nil {
		t.Fatal("registrations were not routed to their workers")
	}
	if workflowComponent.registrations["registeredWorkflowForTest"] == nil ||
		activityComponent.registrations["registeredActivityForTest"] == nil {
		t.Fatal("function registrations did not use their canonical names")
	}
}

func TestNewShutsDownWorkflowWhenActivityCreationFails(t *testing.T) {
	workflowComponent := newFakeComponent()
	activityErr := errors.New("create activity")
	stubWorkerFactories(t,
		func(internalworkflow.Options) (component, error) { return workflowComponent, nil },
		func(internalactivity.Options) (component, error) { return nil, activityErr },
	)

	_, err := New(Options{TaskQueue: "queue"})
	if !errors.Is(err, activityErr) {
		t.Fatalf("New error = %v, want %v", err, activityErr)
	}
	if workflowComponent.shutdowns != 1 {
		t.Fatalf("workflow shutdowns = %d, want 1", workflowComponent.shutdowns)
	}
}

func TestNewCreatesCombinedWorkerWithDefaultEagerReservations(t *testing.T) {
	var gotOptions Options
	var gotWorkflowOptions internalworkflow.Options
	var gotActivityOptions internalactivity.Options
	var gotMaxEager int32
	combined := &fakeCombinedCore{}
	stubWorkerFactories(t,
		func(internalworkflow.Options) (component, error) {
			t.Fatal("separate workflow constructor should not be used")
			return nil, nil
		},
		func(internalactivity.Options) (component, error) {
			t.Fatal("separate activity constructor should not be used")
			return nil, nil
		},
	)
	newWorkflowWorkerWithCore = func(options internalworkflow.Options, core workflowCore) (component, error) {
		gotWorkflowOptions = options
		return &fakeComponent{onShutdown: core.Shutdown}, nil
	}
	newActivityWorkerWithCore = func(options internalactivity.Options, core activityCore) (component, error) {
		gotActivityOptions = options
		return &fakeComponent{onShutdown: core.Shutdown}, nil
	}
	newCombinedCoreWorker = func(
		options Options,
		activityOptions internalactivity.Options,
		workflowOptions internalworkflow.Options,
		maxEager int32,
		_ int32,
	) (combinedCore, error) {
		gotOptions = options
		gotWorkflowOptions = workflowOptions
		gotActivityOptions = activityOptions
		gotMaxEager = maxEager
		return combined, nil
	}

	w, err := New(Options{TaskQueue: "payments"})
	if err != nil {
		t.Fatal(err)
	}
	if gotWorkflowOptions.TaskQueue != "payments" || gotActivityOptions.TaskQueue != "payments" {
		t.Fatalf("combined worker options were not forwarded: workflow=%q activity=%q", gotWorkflowOptions.TaskQueue, gotActivityOptions.TaskQueue)
	}
	if gotOptions.Address != "localhost:7233" || gotOptions.Namespace != "default" ||
		gotOptions.Identity != "go-sdk-on-wasm-core" || gotOptions.PayloadConverter == nil || gotOptions.PayloadCodec == nil {
		t.Fatalf("combined worker connection defaults were not applied: %+v", gotOptions)
	}
	if gotWorkflowOptions.MaxConcurrentWorkflowTaskExecutionSize != 1_000 ||
		gotWorkflowOptions.MaxCachedWorkflows != 32 ||
		gotActivityOptions.MaxConcurrentActivityExecutionSize != 1_000 {
		t.Fatalf("combined worker concurrency defaults were not applied: workflow=%+v activity=%+v", gotWorkflowOptions, gotActivityOptions)
	}
	if gotMaxEager != defaultMaxEagerActivityReservationsPerWorkflowTask {
		t.Fatalf("max eager reservations = %d, want %d", gotMaxEager, defaultMaxEagerActivityReservationsPerWorkflowTask)
	}
	if err := w.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if combined.shutdowns != 1 {
		t.Fatalf("combined core shutdowns = %d, want 1", combined.shutdowns)
	}
}

func TestNewForwardsExplicitEagerReservationsToCombinedCore(t *testing.T) {
	stubWorkerFactories(t,
		func(internalworkflow.Options) (component, error) { return newFakeComponent(), nil },
		func(internalactivity.Options) (component, error) { return newFakeComponent(), nil },
	)
	newWorkflowWorkerWithCore = func(internalworkflow.Options, workflowCore) (component, error) {
		return &fakeComponent{}, nil
	}
	newActivityWorkerWithCore = func(internalactivity.Options, activityCore) (component, error) {
		return &fakeComponent{}, nil
	}

	var gotMaxEager int32
	newCombinedCoreWorker = func(_ Options, _ internalactivity.Options, _ internalworkflow.Options, maxEager int32, _ int32) (combinedCore, error) {
		gotMaxEager = maxEager
		return &fakeCombinedCore{}, nil
	}

	if _, err := New(Options{TaskQueue: "payments", MaxEagerActivityReservationsPerWorkflowTask: 5}); err != nil {
		t.Fatal(err)
	}
	if gotMaxEager != 5 {
		t.Fatalf("max eager reservations = %d, want 5", gotMaxEager)
	}
}

func TestNewForwardsWorkerHeartbeatIntervalToCombinedCore(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		options  Options
		wantMils int32
	}{
		{
			name:     "defaults to reporting once a minute",
			options:  Options{TaskQueue: "payments"},
			wantMils: 60_000,
		},
		{
			name:     "forwards an explicit interval",
			options:  Options{TaskQueue: "payments", WorkerHeartbeatInterval: 5 * time.Second},
			wantMils: 5_000,
		},
		{
			name:     "disabling reporting sends zero",
			options:  Options{TaskQueue: "payments", DisableWorkerHeartbeat: true},
			wantMils: 0,
		},
		{
			name: "disabling reporting overrides an explicit interval",
			options: Options{
				TaskQueue:               "payments",
				WorkerHeartbeatInterval: 5 * time.Second,
				DisableWorkerHeartbeat:  true,
			},
			wantMils: 0,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			stubWorkerFactories(t,
				func(internalworkflow.Options) (component, error) { return newFakeComponent(), nil },
				func(internalactivity.Options) (component, error) { return newFakeComponent(), nil },
			)
			newWorkflowWorkerWithCore = func(internalworkflow.Options, workflowCore) (component, error) {
				return &fakeComponent{}, nil
			}
			newActivityWorkerWithCore = func(internalactivity.Options, activityCore) (component, error) {
				return &fakeComponent{}, nil
			}

			var gotHeartbeatMillis int32
			newCombinedCoreWorker = func(
				_ Options,
				_ internalactivity.Options,
				_ internalworkflow.Options,
				_ int32,
				heartbeatMillis int32,
			) (combinedCore, error) {
				gotHeartbeatMillis = heartbeatMillis
				return &fakeCombinedCore{}, nil
			}

			if _, err := New(testCase.options); err != nil {
				t.Fatal(err)
			}
			if gotHeartbeatMillis != testCase.wantMils {
				t.Fatalf("worker heartbeat interval = %dms, want %dms", gotHeartbeatMillis, testCase.wantMils)
			}
		})
	}
}

func TestNewRejectsWorkerHeartbeatIntervalOutsideCoreRange(t *testing.T) {
	for _, interval := range []time.Duration{time.Millisecond, 999 * time.Millisecond, time.Minute + time.Second} {
		stubWorkerFactories(t,
			func(internalworkflow.Options) (component, error) {
				t.Fatal("workflow constructor should not be called for an invalid heartbeat interval")
				return nil, nil
			},
			func(internalactivity.Options) (component, error) {
				t.Fatal("activity constructor should not be called for an invalid heartbeat interval")
				return nil, nil
			},
		)
		newCombinedCoreWorker = func(
			Options, internalactivity.Options, internalworkflow.Options, int32, int32,
		) (combinedCore, error) {
			t.Fatal("combined constructor should not be called for an invalid heartbeat interval")
			return nil, nil
		}

		_, err := New(Options{TaskQueue: "payments", WorkerHeartbeatInterval: interval})
		if err == nil || err.Error() != "worker heartbeat interval must be between 1s and 1m0s" {
			t.Fatalf("New(%s) error = %v", interval, err)
		}
	}
}

func TestNewRejectsInvalidEagerReservationsForCombinedWorker(t *testing.T) {
	stubWorkerFactories(t,
		func(internalworkflow.Options) (component, error) {
			t.Fatal("workflow constructor should not be called for invalid eager options")
			return nil, nil
		},
		func(internalactivity.Options) (component, error) {
			t.Fatal("activity constructor should not be called for invalid eager options")
			return nil, nil
		},
	)
	newCombinedCoreWorker = func(Options, internalactivity.Options, internalworkflow.Options, int32, int32) (combinedCore, error) {
		t.Fatal("combined constructor should not be called for invalid eager options")
		return nil, nil
	}

	_, err := New(Options{TaskQueue: "payments", MaxEagerActivityReservationsPerWorkflowTask: -1})
	if err == nil || err.Error() != "maximum eager activity reservations per workflow task must be between 1 and 2147483647" {
		t.Fatalf("New error = %v", err)
	}
}

func TestNewReleasesCombinedCoreWhenSecondWorkerCreationFails(t *testing.T) {
	combined := &fakeCombinedCore{}
	workflowComponent := newFakeComponent()
	activityErr := errors.New("create activity")
	stubWorkerFactories(t,
		func(internalworkflow.Options) (component, error) {
			t.Fatal("separate workflow constructor should not be used")
			return nil, nil
		},
		func(internalactivity.Options) (component, error) {
			t.Fatal("separate activity constructor should not be used")
			return nil, nil
		},
	)
	newWorkflowWorkerWithCore = func(_ internalworkflow.Options, core workflowCore) (component, error) {
		workflowComponent.onShutdown = core.Shutdown
		return workflowComponent, nil
	}
	newActivityWorkerWithCore = func(internalactivity.Options, activityCore) (component, error) {
		return nil, activityErr
	}
	newCombinedCoreWorker = func(Options, internalactivity.Options, internalworkflow.Options, int32, int32) (combinedCore, error) {
		return combined, nil
	}

	_, err := New(Options{TaskQueue: "payments"})
	if err == nil || !errors.Is(err, activityErr) {
		t.Fatalf("New error = %v", err)
	}
	if workflowComponent.shutdowns != 1 {
		t.Fatalf("workflow shutdowns = %d, want 1", workflowComponent.shutdowns)
	}
	if combined.shutdowns != 1 {
		t.Fatalf("combined core shutdowns = %d, want 1", combined.shutdowns)
	}
}

func TestRunCancelsPeerAfterWorkerFailure(t *testing.T) {
	runErr := errors.New("workflow failed")
	workflowComponent := newFakeComponent()
	workflowComponent.run = func(context.Context) error { return runErr }
	activityComponent := newFakeComponent()
	activityStopped := make(chan struct{})
	activityComponent.run = func(ctx context.Context) error {
		<-ctx.Done()
		close(activityStopped)
		return nil
	}
	w := &Worker{workflow: workflowComponent, activity: activityComponent}

	if err := w.Run(context.Background()); !errors.Is(err, runErr) {
		t.Fatalf("Run error = %v, want %v", err, runErr)
	}
	select {
	case <-activityStopped:
	default:
		t.Fatal("activity worker was not cancelled")
	}
}

func TestShutdownStopsBothWorkersAndJoinsErrors(t *testing.T) {
	workflowErr := errors.New("workflow shutdown")
	activityErr := errors.New("activity shutdown")
	workflowComponent := newFakeComponent()
	workflowComponent.shutdownErr = workflowErr
	activityComponent := newFakeComponent()
	activityComponent.shutdownErr = activityErr
	w := &Worker{workflow: workflowComponent, activity: activityComponent}

	err := w.Shutdown()
	if !errors.Is(err, workflowErr) || !errors.Is(err, activityErr) {
		t.Fatalf("Shutdown error = %v, want both component errors", err)
	}
	if workflowComponent.shutdowns != 1 || activityComponent.shutdowns != 1 {
		t.Fatalf("shutdowns = workflow %d, activity %d", workflowComponent.shutdowns, activityComponent.shutdowns)
	}
}
