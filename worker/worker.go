// Package worker hosts workflow and activity implementations on Temporal Core.
package worker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	internalactivity "github.com/temporalio/sdk-go-on-wasm-core/internal/activityworker"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/corebridge"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/functionname"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/workerconfig"
	internalworkflow "github.com/temporalio/sdk-go-on-wasm-core/internal/workflowworker"
	commonpb "go.temporal.io/api/common/v1"
)

// PayloadConverter converts between Go values and unencoded Temporal payloads.
// Payload codecs are configured separately and run at the Core boundary.
type PayloadConverter interface {
	ToPayload(value any) (*commonpb.Payload, error)
	FromPayload(payload *commonpb.Payload, valuePtr any) error
	ToPayloads(values ...any) (*commonpb.Payloads, error)
	FromPayloads(payloads *commonpb.Payloads, valuePtrs ...any) error
}

// PayloadCodec transforms serialized payloads before they cross the Core boundary.
// Implementations must not mutate input payloads and must be safe for concurrent use.
type PayloadCodec interface {
	Encode([]*commonpb.Payload) ([]*commonpb.Payload, error)
	Decode([]*commonpb.Payload) ([]*commonpb.Payload, error)
}

// Options configures a Core-backed worker.
type Options struct {
	Address          string
	Namespace        string
	TaskQueue        string
	Identity         string
	PayloadConverter PayloadConverter
	PayloadCodec     PayloadCodec
	// TLS configures transport security. APIKey also enables TLS when TLS is nil.
	TLS *tls.Config
	// APIKey authenticates every request with an Authorization: Bearer header.
	APIKey string
	// MaxConcurrentActivityExecutionSize limits activity slots managed by Core.
	// The default is 1000.
	MaxConcurrentActivityExecutionSize int
	// MaxConcurrentActivityTaskPollers sets a fixed Core activity poller maximum.
	// A zero value lets Core select fixed or autoscaling pollers.
	MaxConcurrentActivityTaskPollers int
	// MaxConcurrentWorkflowTaskExecutionSize limits workflow task slots managed by Core.
	// The default is 1000.
	MaxConcurrentWorkflowTaskExecutionSize int
	// MaxConcurrentWorkflowTaskPollers sets a fixed Core workflow poller maximum.
	// A zero value leaves the setting unspecified so Core chooses it.
	MaxConcurrentWorkflowTaskPollers int
	// MaxCachedWorkflows limits the workflow runs cached by Core. The default is 32.
	MaxCachedWorkflows int
	// MaxEagerActivityReservationsPerWorkflowTask limits eager activity slots per
	// workflow task. The default is 3. Core can dispatch activities eagerly because
	// each Worker polls both workflow and activity tasks on the same task queue.
	MaxEagerActivityReservationsPerWorkflowTask int
	// WorkerHeartbeatInterval sets how often Core reports this worker's status to the
	// server, which is what makes the worker visible to Temporal's worker management
	// APIs. The default is one minute, and Core requires a value between one second
	// and one minute. Servers that do not advertise the worker heartbeat capability
	// ignore the reports.
	WorkerHeartbeatInterval time.Duration
	// DisableWorkerHeartbeat stops the worker from reporting its status. It takes
	// precedence over WorkerHeartbeatInterval.
	DisableWorkerHeartbeat bool
}

type component interface {
	Register(string, any)
	Run(context.Context) error
	Shutdown() error
}

type workflowCore interface {
	PollWorkflow(context.Context) ([]byte, error)
	CompleteWorkflow([]byte) error
	Shutdown() error
}

type activityCore interface {
	PollActivity(context.Context) ([]byte, error)
	CompleteActivity([]byte) error
	RecordActivityHeartbeat([]byte) error
	Shutdown() error
}

type combinedCore interface {
	workflowCore
	activityCore
}

var (
	newActivityWorker = func(options internalactivity.Options) (component, error) {
		return internalactivity.New(options)
	}
	newActivityWorkerWithCore = func(options internalactivity.Options, core activityCore) (component, error) {
		return internalactivity.NewWithCore(options, core)
	}
	newWorkflowWorker = func(options internalworkflow.Options) (component, error) {
		return internalworkflow.New(options)
	}
	newWorkflowWorkerWithCore = func(options internalworkflow.Options, core workflowCore) (component, error) {
		return internalworkflow.NewWithCore(options, core)
	}
	newCombinedCoreWorker = func(
		options Options,
		activityOptions internalactivity.Options,
		workflowOptions internalworkflow.Options,
		maxEagerActivityReservationsPerWorkflowTask int32,
		workerHeartbeatIntervalMillis int32,
	) (combinedCore, error) {
		return corebridge.NewCombined(
			options.Address,
			options.Namespace,
			options.TaskQueue,
			options.Identity,
			int32(activityOptions.MaxConcurrentActivityExecutionSize),
			int32(activityOptions.MaxConcurrentActivityTaskPollers),
			corebridge.WorkflowOptions{
				MaxConcurrentWorkflowTaskExecutions:         int32(workflowOptions.MaxConcurrentWorkflowTaskExecutionSize),
				MaxConcurrentWorkflowTaskPollers:            int32(workflowOptions.MaxConcurrentWorkflowTaskPollers),
				MaxCachedWorkflows:                          int32(workflowOptions.MaxCachedWorkflows),
				MaxEagerActivityReservationsPerWorkflowTask: maxEagerActivityReservationsPerWorkflowTask,
				WorkerHeartbeatIntervalMillis:               workerHeartbeatIntervalMillis,
				Connection: corebridge.ConnectionOptions{
					TLS:    options.TLS,
					APIKey: options.APIKey,
				},
			},
		)
	}
)

const defaultMaxEagerActivityReservationsPerWorkflowTask = 3

// Core accepts worker heartbeat intervals between one second and one minute, and
// defaults to reporting once a minute.
const (
	defaultWorkerHeartbeatInterval = time.Minute
	minWorkerHeartbeatInterval     = time.Second
	maxWorkerHeartbeatInterval     = time.Minute
)

// Worker hosts registered workflow and activity implementations.
type Worker struct {
	activity component
	workflow component
}

// New creates a worker that polls the task queue for both workflow and activity tasks.
func New(options Options) (*Worker, error) {
	return newWorker(options)
}

func newWorker(options Options) (*Worker, error) {
	workflowOptions := internalworkflow.Options{
		Address:                                options.Address,
		Namespace:                              options.Namespace,
		TaskQueue:                              options.TaskQueue,
		Identity:                               options.Identity,
		PayloadConverter:                       options.PayloadConverter,
		PayloadCodec:                           options.PayloadCodec,
		TLS:                                    options.TLS,
		APIKey:                                 options.APIKey,
		MaxConcurrentWorkflowTaskExecutionSize: options.MaxConcurrentWorkflowTaskExecutionSize,
		MaxConcurrentWorkflowTaskPollers:       options.MaxConcurrentWorkflowTaskPollers,
		MaxCachedWorkflows:                     options.MaxCachedWorkflows,
	}
	activityOptions := internalactivity.Options{
		Address:                            options.Address,
		Namespace:                          options.Namespace,
		TaskQueue:                          options.TaskQueue,
		Identity:                           options.Identity,
		PayloadConverter:                   options.PayloadConverter,
		PayloadCodec:                       options.PayloadCodec,
		TLS:                                options.TLS,
		APIKey:                             options.APIKey,
		MaxConcurrentActivityExecutionSize: options.MaxConcurrentActivityExecutionSize,
		MaxConcurrentActivityTaskPollers:   options.MaxConcurrentActivityTaskPollers,
	}
	w := &Worker{}
	if newCombinedCoreWorker != nil {
		connection, err := workerconfig.Normalize("worker", workerconfig.Connection{
			Address:          options.Address,
			Namespace:        options.Namespace,
			TaskQueue:        options.TaskQueue,
			Identity:         options.Identity,
			PayloadConverter: options.PayloadConverter,
			PayloadCodec:     options.PayloadCodec,
			TLS:              options.TLS,
			APIKey:           options.APIKey,
		})
		if err != nil {
			return nil, err
		}
		options.Address = connection.Address
		options.Namespace = connection.Namespace
		options.TaskQueue = connection.TaskQueue
		options.Identity = connection.Identity
		options.PayloadConverter = connection.PayloadConverter
		options.PayloadCodec = connection.PayloadCodec
		workflowOptions.Address = connection.Address
		workflowOptions.Namespace = connection.Namespace
		workflowOptions.TaskQueue = connection.TaskQueue
		workflowOptions.Identity = connection.Identity
		workflowOptions.PayloadConverter = connection.PayloadConverter
		workflowOptions.PayloadCodec = connection.PayloadCodec
		activityOptions.Address = connection.Address
		activityOptions.Namespace = connection.Namespace
		activityOptions.TaskQueue = connection.TaskQueue
		activityOptions.Identity = connection.Identity
		activityOptions.PayloadConverter = connection.PayloadConverter
		activityOptions.PayloadCodec = connection.PayloadCodec
		activityOptions, workflowOptions, err := normalizeCombinedCoreOptions(activityOptions, workflowOptions)
		if err != nil {
			return nil, err
		}
		maxEager, err := normalizeMaxEagerActivityReservations(options.MaxEagerActivityReservationsPerWorkflowTask)
		if err != nil {
			return nil, err
		}
		heartbeatMillis, err := normalizeWorkerHeartbeatInterval(
			options.WorkerHeartbeatInterval,
			options.DisableWorkerHeartbeat,
		)
		if err != nil {
			return nil, err
		}
		core, err := newCombinedCoreWorker(options, activityOptions, workflowOptions, maxEager, heartbeatMillis)
		if err != nil {
			return nil, err
		}
		if core == nil {
			return nil, errors.New("combined core worker factory returned nil")
		}
		sharedCore := newSharedCombinedCore(core)
		workflowCore := sharedCore.workflow()
		workflow, err := newWorkflowWorkerWithCore(workflowOptions, workflowCore)
		if err != nil {
			_ = workflowCore.Shutdown()
			return nil, err
		}
		w.workflow = workflow
		activityCore := sharedCore.activity()
		activity, err := newActivityWorkerWithCore(activityOptions, activityCore)
		if err != nil {
			err = errors.Join(err, activityCore.Shutdown(), w.workflow.Shutdown())
			return nil, err
		}
		w.activity = activity
		return w, nil
	}
	workflow, err := newWorkflowWorker(workflowOptions)
	if err != nil {
		return nil, err
	}
	w.workflow = workflow
	activity, err := newActivityWorker(activityOptions)
	if err != nil {
		return nil, errors.Join(err, w.workflow.Shutdown())
	}
	w.activity = activity
	return w, nil
}

func normalizeCombinedCoreOptions(
	activityOptions internalactivity.Options,
	workflowOptions internalworkflow.Options,
) (internalactivity.Options, internalworkflow.Options, error) {
	if activityOptions.MaxConcurrentActivityExecutionSize == 0 {
		activityOptions.MaxConcurrentActivityExecutionSize = 1_000
	}
	if workflowOptions.MaxConcurrentWorkflowTaskExecutionSize == 0 {
		workflowOptions.MaxConcurrentWorkflowTaskExecutionSize = 1_000
	}
	if workflowOptions.MaxCachedWorkflows == 0 {
		workflowOptions.MaxCachedWorkflows = 32
	}
	values := []struct {
		name      string
		value     int
		allowZero bool
	}{
		{name: "maximum concurrent activity executions", value: activityOptions.MaxConcurrentActivityExecutionSize},
		{name: "maximum concurrent activity task pollers", value: activityOptions.MaxConcurrentActivityTaskPollers, allowZero: true},
		{name: "maximum concurrent workflow task executions", value: workflowOptions.MaxConcurrentWorkflowTaskExecutionSize},
		{name: "maximum concurrent workflow task pollers", value: workflowOptions.MaxConcurrentWorkflowTaskPollers, allowZero: true},
		{name: "maximum cached workflows", value: workflowOptions.MaxCachedWorkflows},
	}
	for _, option := range values {
		minimum := 1
		if option.allowZero {
			minimum = 0
		}
		if option.value < minimum || option.value > math.MaxInt32 {
			return internalactivity.Options{}, internalworkflow.Options{}, fmt.Errorf(
				"%s must be between %d and 2147483647",
				option.name,
				minimum,
			)
		}
	}
	return activityOptions, workflowOptions, nil
}

func normalizeMaxEagerActivityReservations(value int) (int32, error) {
	if value == 0 {
		value = defaultMaxEagerActivityReservationsPerWorkflowTask
	}
	if value < 1 || value > math.MaxInt32 {
		return 0, errors.New(
			"maximum eager activity reservations per workflow task must be between 1 and 2147483647",
		)
	}
	return int32(value), nil
}

// normalizeWorkerHeartbeatInterval converts the configured interval into the
// milliseconds the Core bridge expects. Zero means the worker does not report status.
func normalizeWorkerHeartbeatInterval(interval time.Duration, disabled bool) (int32, error) {
	if disabled {
		return 0, nil
	}
	if interval == 0 {
		interval = defaultWorkerHeartbeatInterval
	}
	if interval < minWorkerHeartbeatInterval || interval > maxWorkerHeartbeatInterval {
		return 0, fmt.Errorf(
			"worker heartbeat interval must be between %s and %s",
			minWorkerHeartbeatInterval,
			maxWorkerHeartbeatInterval,
		)
	}
	return int32(interval.Milliseconds()), nil
}

type sharedCombinedCore struct {
	core combinedCore
	mu   sync.Mutex
	refs int
	err  error
}

func newSharedCombinedCore(core combinedCore) *sharedCombinedCore {
	return &sharedCombinedCore{core: core}
}

func (c *sharedCombinedCore) workflow() workflowCore {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refs++
	return &sharedWorkflowCore{core: c.core, owner: c}
}

func (c *sharedCombinedCore) activity() activityCore {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refs++
	return &sharedActivityCore{core: c.core, owner: c}
}

func (c *sharedCombinedCore) release() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refs == 0 {
		return c.err
	}
	c.refs--
	if c.refs == 0 {
		c.err = c.core.Shutdown()
	}
	return c.err
}

type sharedWorkflowCore struct {
	core         combinedCore
	owner        *sharedCombinedCore
	shutdownOnce sync.Once
	shutdownErr  error
}

func (c *sharedWorkflowCore) PollWorkflow(ctx context.Context) ([]byte, error) {
	return c.core.PollWorkflow(ctx)
}

func (c *sharedWorkflowCore) CompleteWorkflow(encoded []byte) error {
	return c.core.CompleteWorkflow(encoded)
}

func (c *sharedWorkflowCore) Shutdown() error {
	c.shutdownOnce.Do(func() {
		c.shutdownErr = c.owner.release()
	})
	return c.shutdownErr
}

type sharedActivityCore struct {
	core         combinedCore
	owner        *sharedCombinedCore
	shutdownOnce sync.Once
	shutdownErr  error
}

func (c *sharedActivityCore) PollActivity(ctx context.Context) ([]byte, error) {
	return c.core.PollActivity(ctx)
}

func (c *sharedActivityCore) CompleteActivity(encoded []byte) error {
	return c.core.CompleteActivity(encoded)
}

func (c *sharedActivityCore) RecordActivityHeartbeat(encoded []byte) error {
	return c.core.RecordActivityHeartbeat(encoded)
}

func (c *sharedActivityCore) Shutdown() error {
	c.shutdownOnce.Do(func() {
		c.shutdownErr = c.owner.release()
	})
	return c.shutdownErr
}

// RegisterWorkflow registers a workflow implementation under its Go function name.
// The function must accept *workflow.Context first and return error or (result, error).
func (w *Worker) RegisterWorkflow(function any) {
	name, err := functionname.Get("workflow", function)
	if err != nil {
		panic(err)
	}
	w.RegisterWorkflowUntyped(name, function)
}

// RegisterWorkflowUntyped registers a workflow implementation under an explicit name.
func (w *Worker) RegisterWorkflowUntyped(name string, function any) {
	w.workflow.Register(name, function)
}

// RegisterActivity registers an activity implementation under its Go function name.
// The function may accept context.Context first and must return error or (result, error).
func (w *Worker) RegisterActivity(function any) {
	name, err := functionname.Get("activity", function)
	if err != nil {
		panic(err)
	}
	w.RegisterActivityUntyped(name, function)
}

// RegisterActivityUntyped registers an activity implementation under an explicit name.
func (w *Worker) RegisterActivityUntyped(name string, function any) {
	w.activity.Register(name, function)
}

// Run polls and processes tasks until ctx is cancelled or a worker fails.
func (w *Worker) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- w.workflow.Run(runCtx) }()
	go func() { results <- w.activity.Run(runCtx) }()
	first := <-results
	cancel()
	second := <-results
	return errors.Join(first, second)
}

// Shutdown stops the Core workers. Run also shuts them down before returning.
func (w *Worker) Shutdown() error {
	return errors.Join(w.workflow.Shutdown(), w.activity.Shutdown())
}
