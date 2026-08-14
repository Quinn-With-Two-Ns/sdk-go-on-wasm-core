// Package workflowworker implements a replayable workflow interpreter with deterministic
// workflow coroutines and future-based timers, remote activities, and child workflows.
package workflowworker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"iter"
	"math"
	"runtime/debug"
	"sync"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/completionpipe"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/corebridge"
	activityresult "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/activityresult"
	childworkflow "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/childworkflow"
	workflowactivation "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowactivation"
	workflowcommands "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcommands"
	workflowcompletion "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcompletion"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/workerconfig"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/proto"
)

type coreWorker interface {
	PollWorkflow(context.Context) ([]byte, error)
	CompleteWorkflow([]byte) error
	Shutdown() error
}

type workflowPollEvent struct {
	activation      *workflowactivation.WorkflowActivation
	payloadCodecErr error
	err             error
}

type workflowProcessorResult struct {
	completion *workflowcompletion.WorkflowActivationCompletion
	runID      string
	runTurn    *workflowRunTurn
}

type workflowCoreOptions struct {
	maxConcurrentWorkflowTaskExecutions int32
	maxConcurrentWorkflowTaskPollers    int32
	maxCachedWorkflows                  int32
	tls                                 *tls.Config
	apiKey                              string
}

type workflowRunLane struct {
	refs int
	tail <-chan struct{}
}

type workflowRunTurn struct {
	lane  *workflowRunLane
	ready <-chan struct{}
	done  chan struct{}
}

const (
	defaultMaxConcurrentWorkflowTaskExecutions = 1000
	defaultMaxCachedWorkflows                  = 32
)

var (
	newWorkflowCoreWorker = instantiateWorkflowCoreWorker
	workflowRunLockHook   func(runID string, phase string)
)

// Options configures a workflow-only Core worker.
type Options struct {
	Address          string
	Namespace        string
	TaskQueue        string
	Identity         string
	PayloadConverter converter.PayloadConverter
	PayloadCodec     converter.PayloadCodec
	// TLS configures transport security. APIKey also enables TLS when TLS is nil.
	TLS *tls.Config
	// APIKey authenticates every request with an Authorization: Bearer header.
	APIKey string
	// MaxConcurrentWorkflowTaskExecutionSize limits the workflow task slots
	// managed by Core. The default is 1000.
	MaxConcurrentWorkflowTaskExecutionSize int
	// MaxConcurrentWorkflowTaskPollers sets a fixed Core workflow task poller
	// maximum. A zero value leaves the setting unspecified so Core chooses it.
	MaxConcurrentWorkflowTaskPollers int
	// MaxCachedWorkflows limits the number of cached workflow runs kept by Core.
	// The default is 32.
	MaxCachedWorkflows int
}

// Worker owns the Go-side workflow run cache and activation loop.
type Worker struct {
	core                                coreWorker
	payloadConverter                    converter.PayloadConverter
	payloadCodec                        converter.PayloadCodec
	namespace                           string
	taskQueue                           string
	handlers                            map[string]*registeredWorkflow
	maxConcurrentWorkflowTaskExecutions int

	runsMu       sync.Mutex
	runs         map[string]*workflowExecution
	runLocks     map[string]*workflowRunLane
	shutdownOnce sync.Once
	shutdownErr  error
}

// New creates a workflow-only worker backed by Temporal Core compiled to WASM.
func New(options Options) (*Worker, error) {
	connection, err := workerconfig.Normalize("workflow", workerconfig.Connection{
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
	normalizedOptions, err := normalizeWorkflowOptions(options)
	if err != nil {
		return nil, err
	}
	core, err := newWorkflowCoreWorker(
		connection.Address,
		connection.Namespace,
		connection.TaskQueue,
		connection.Identity,
		normalizedOptions,
	)
	if err != nil {
		return nil, err
	}
	return newWorkerWithCore(connection, normalizedOptions, core), nil
}

// NewWithCore creates a workflow worker that reuses an existing Core worker.
func NewWithCore(options Options, core interface {
	PollWorkflow(context.Context) ([]byte, error)
	CompleteWorkflow([]byte) error
	Shutdown() error
}) (*Worker, error) {
	connection, err := workerconfig.Normalize("workflow", workerconfig.Connection{
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
	normalizedOptions, err := normalizeWorkflowOptions(options)
	if err != nil {
		return nil, err
	}
	return newWorkerWithCore(connection, normalizedOptions, core), nil
}

func newWorkerWithCore(
	connection workerconfig.Connection,
	normalizedOptions workflowCoreOptions,
	core coreWorker,
) *Worker {
	return &Worker{
		core:                                core,
		payloadConverter:                    connection.PayloadConverter,
		payloadCodec:                        connection.PayloadCodec,
		namespace:                           connection.Namespace,
		taskQueue:                           connection.TaskQueue,
		handlers:                            make(map[string]*registeredWorkflow),
		maxConcurrentWorkflowTaskExecutions: int(normalizedOptions.maxConcurrentWorkflowTaskExecutions),
		runs:                                make(map[string]*workflowExecution),
		runLocks:                            make(map[string]*workflowRunLane),
	}
}

// Register registers a deterministic workflow function. It must accept *Context first and return
// error or (result, error). The result is encoded with the worker's payload converter.
func (w *Worker) Register(name string, function any) {
	if _, exists := w.handlers[name]; exists {
		panic("workflow is already registered: " + name)
	}
	workflow, err := newRegisteredWorkflow(name, function)
	if err != nil {
		panic(err)
	}
	w.handlers[name] = workflow
}

// Run polls and completes workflow activations until ctx is cancelled or Core returns an error.
func (w *Worker) Run(ctx context.Context) error {
	return newWorkflowRunLoop(w, ctx).run()
}

// Shutdown stops cached executions and gracefully shuts down Core.
func (w *Worker) Shutdown() error {
	w.stopAllRuns()
	return w.shutdownCore()
}

func (w *Worker) handleActivation(activation *workflowactivation.WorkflowActivation) *workflowcompletion.WorkflowActivationCompletion {
	if activation.RunId == "" {
		return failedActivation("", errors.New("workflow activation is missing a run ID"))
	}
	runTurn := w.reserveRunTurn(activation.RunId)
	runTurn.wait()
	defer w.releaseRunTurn(activation.RunId, runTurn)
	return w.handleActivationLocked(activation)
}

func (w *Worker) handleActivationLocked(activation *workflowactivation.WorkflowActivation) *workflowcompletion.WorkflowActivationCompletion {
	if workflowRunLockHook != nil {
		workflowRunLockHook(activation.RunId, "enter")
		defer workflowRunLockHook(activation.RunId, "exit")
	}
	if len(activation.Jobs) == 1 && activation.Jobs[0].GetRemoveFromCache() != nil {
		if execution := w.getRun(activation.RunId); execution != nil {
			execution.stop()
			w.deleteRun(activation.RunId)
		}
		return successfulActivation(activation.RunId, nil)
	}

	execution := w.getRun(activation.RunId)
	jobs := activation.Jobs
	if len(jobs) > 0 {
		if initialize := jobs[0].GetInitializeWorkflow(); initialize != nil {
			if execution != nil {
				return failedActivation(activation.RunId, fmt.Errorf(
					"workflow run %q was initialized twice without eviction",
					activation.RunId,
				))
			}
			handler := w.handlers[initialize.WorkflowType]
			if handler == nil {
				return failedActivation(activation.RunId, fmt.Errorf("workflow %q is not registered", initialize.WorkflowType))
			}
			execution = newWorkflowExecution(w.payloadConverter, w.namespace, w.taskQueue, activation.RunId)
			w.setRun(activation.RunId, execution)
			execution.start(handler, initialize.Arguments)
			jobs = jobs[1:]
		}
	}
	if execution == nil {
		return failedActivation(activation.RunId, fmt.Errorf(
			"workflow run %q is not cached and activation has no initializer",
			activation.RunId,
		))
	}
	commands, err := execution.activate(jobs)
	if err != nil {
		execution.stop()
		w.deleteRun(activation.RunId)
		return failedActivation(activation.RunId, err)
	}
	return successfulActivation(activation.RunId, commands)
}

func successfulActivation(runID string, commands []*workflowcommands.WorkflowCommand) *workflowcompletion.WorkflowActivationCompletion {
	return &workflowcompletion.WorkflowActivationCompletion{
		RunId: runID,
		Status: &workflowcompletion.WorkflowActivationCompletion_Successful{
			Successful: &workflowcompletion.Success{Commands: commands},
		},
	}
}

func failedActivation(runID string, err error) *workflowcompletion.WorkflowActivationCompletion {
	return &workflowcompletion.WorkflowActivationCompletion{
		RunId: runID,
		Status: &workflowcompletion.WorkflowActivationCompletion_Failed{
			Failed: &workflowcompletion.Failure{Failure: newFailure(err, "GoWasmWorkflowActivationError", "")},
		},
	}
}

func newFailure(err error, failureType, stackTrace string) *failurepb.Failure {
	return &failurepb.Failure{
		Message:    err.Error(),
		Source:     "Go SDK on WASM Core prototype",
		StackTrace: stackTrace,
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: failureType},
		},
	}
}

func failureMessage(failure *failurepb.Failure) string {
	if failure == nil || failure.Message == "" {
		return "unknown failure"
	}
	return failure.Message
}

type operationKind int

const (
	operationTimer operationKind = iota
	operationActivity
	operationChild
)

type workflowOperation struct {
	kind       operationKind
	result     *futureImpl
	started    *futureImpl
	workflowID string
}

type workflowExecution struct {
	payloadConverter converter.PayloadConverter
	namespace        string
	taskQueue        string
	runID            string
	nextSequence     uint32
	operations       map[uint32]*workflowOperation
	signalChannels   map[string]*signalChannel
	dispatcher       workflowDispatcher
	commands         []*workflowcommands.WorkflowCommand
	terminal         bool
	started          bool
	stopped          bool
}

type workflowDispatcher struct {
	coroutines []*workflowCoroutine
	current    *workflowCoroutine
}

type workflowCoroutine struct {
	context  *Context
	next     func() (struct{}, bool)
	stopPull func()
	yield    func(struct{}) bool
	waiting  *futureImpl
	done     bool
	err      error
}

func newWorkflowExecution(payloadConverter converter.PayloadConverter, namespace, taskQueue, runID string) *workflowExecution {
	execution := &workflowExecution{
		payloadConverter: payloadConverter,
		namespace:        namespace,
		taskQueue:        taskQueue,
		runID:            runID,
		operations:       make(map[uint32]*workflowOperation),
		signalChannels:   make(map[string]*signalChannel),
	}
	return execution
}

func (e *workflowExecution) start(handler *registeredWorkflow, input []*commonpb.Payload) {
	e.started = true
	e.addCoroutine("root", func(ctx *Context) {
		result, err := e.executeSafely(handler, ctx, input)
		if err != nil {
			e.failWorkflow(err)
			return
		}
		e.emit(&workflowcommands.WorkflowCommand{
			Variant: &workflowcommands.WorkflowCommand_CompleteWorkflowExecution{
				CompleteWorkflowExecution: &workflowcommands.CompleteWorkflowExecution{Result: result},
			},
		})
		e.terminal = true
	})
}

func (e *workflowExecution) executeSafely(
	handler *registeredWorkflow,
	ctx *Context,
	input []*commonpb.Payload,
) (result *commonpb.Payload, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("workflow %q panicked: %v\n%s", handler.name, recovered, debug.Stack())
		}
	}()
	return handler.execute(ctx, e.payloadConverter, input)
}

func (e *workflowExecution) addCoroutine(name string, function func(*Context)) *workflowCoroutine {
	coroutine := &workflowCoroutine{}
	coroutine.context = &Context{execution: e, coroutine: coroutine}
	coroutine.next, coroutine.stopPull = iter.Pull(func(yield func(struct{}) bool) {
		coroutine.yield = yield
		defer func() {
			if recovered := recover(); recovered != nil {
				coroutine.err = fmt.Errorf("workflow coroutine %q panicked: %v\n%s", name, recovered, debug.Stack())
			}
		}()
		function(coroutine.context)
	})
	e.dispatcher.coroutines = append(e.dispatcher.coroutines, coroutine)
	return coroutine
}

func (e *workflowExecution) activate(jobs []*workflowactivation.WorkflowActivationJob) ([]*workflowcommands.WorkflowCommand, error) {
	if e.stopped {
		return nil, errors.New("workflow execution is stopped")
	}
	if !e.started {
		return nil, errors.New("workflow execution was not initialized")
	}
	if err := e.drive(); err != nil {
		return nil, err
	}

	pending := append([]*workflowactivation.WorkflowActivationJob(nil), jobs...)
	for len(pending) > 0 {
		remaining := pending[:0]
		applied := false
		for _, job := range pending {
			matched, err := e.tryApply(job)
			if err != nil {
				return nil, err
			}
			if matched {
				applied = true
			} else {
				remaining = append(remaining, job)
			}
		}
		pending = remaining
		if len(pending) == 0 {
			break
		}
		beforeSequence := e.nextSequence
		beforeOperations := len(e.operations)
		if err := e.drive(); err != nil {
			return nil, err
		}
		if !applied && beforeSequence == e.nextSequence && beforeOperations == len(e.operations) {
			return nil, fmt.Errorf("workflow activation job %T does not match an outstanding operation", pending[0].Variant)
		}
	}
	if err := e.drive(); err != nil {
		return nil, err
	}
	commands := e.commands
	e.commands = nil
	return commands, nil
}

func (e *workflowExecution) tryApply(job *workflowactivation.WorkflowActivationJob) (bool, error) {
	if job.GetInitializeWorkflow() != nil {
		return false, errors.New("InitializeWorkflow must be the first job in an activation")
	}
	if job.GetRemoveFromCache() != nil {
		return false, errors.New("RemoveFromCache must be the only job in an activation")
	}
	if signal := job.GetSignalWorkflow(); signal != nil {
		// Signals are addressed by name rather than by command sequence, so they always apply and
		// buffer even when no coroutine has asked for that name yet.
		e.signalChannel(signal.SignalName).deliverSignal(signal.Input)
		return true, nil
	}
	if fired := job.GetFireTimer(); fired != nil {
		operation := e.operations[fired.Seq]
		if operation == nil {
			return false, nil
		}
		if operation.kind != operationTimer {
			return false, fmt.Errorf("FireTimer sequence %d targets a non-timer operation", fired.Seq)
		}
		operation.result.set(nil, nil)
		delete(e.operations, fired.Seq)
		return true, nil
	}
	if resolved := job.GetResolveActivity(); resolved != nil {
		operation := e.operations[resolved.Seq]
		if operation == nil {
			return false, nil
		}
		if operation.kind != operationActivity {
			return false, fmt.Errorf("ResolveActivity sequence %d targets a non-activity operation", resolved.Seq)
		}
		value, err := activityResolution(resolved.Result)
		operation.result.set(value, err)
		delete(e.operations, resolved.Seq)
		return true, nil
	}
	if resolved := job.GetResolveChildWorkflowExecutionStart(); resolved != nil {
		operation := e.operations[resolved.Seq]
		if operation == nil {
			return false, nil
		}
		if operation.kind != operationChild {
			return false, fmt.Errorf("child start sequence %d targets a non-child operation", resolved.Seq)
		}
		if success := resolved.GetSucceeded(); success != nil {
			operation.started.set(WorkflowExecution{ID: operation.workflowID, RunID: success.RunId}, nil)
			return true, nil
		}
		var err error
		if failed := resolved.GetFailed(); failed != nil {
			err = fmt.Errorf(
				"child workflow %q (%q) failed to start: %s",
				failed.WorkflowId,
				failed.WorkflowType,
				failed.Cause,
			)
		} else if cancelled := resolved.GetCancelled(); cancelled != nil {
			err = fmt.Errorf("child workflow %q start cancelled: %s", operation.workflowID, failureMessage(cancelled.Failure))
		} else {
			return false, fmt.Errorf("child workflow %q start resolution has no status", operation.workflowID)
		}
		operation.started.set(nil, err)
		operation.result.set(nil, err)
		delete(e.operations, resolved.Seq)
		return true, nil
	}
	if resolved := job.GetResolveChildWorkflowExecution(); resolved != nil {
		operation := e.operations[resolved.Seq]
		if operation == nil {
			return false, nil
		}
		if operation.kind != operationChild {
			return false, fmt.Errorf("child completion sequence %d targets a non-child operation", resolved.Seq)
		}
		if !operation.started.ready || operation.started.err != nil {
			return false, fmt.Errorf("child workflow %q completed before its start resolved", operation.workflowID)
		}
		value, err := childWorkflowResolution(resolved.Result)
		operation.result.set(value, err)
		delete(e.operations, resolved.Seq)
		return true, nil
	}
	return false, fmt.Errorf("workflow activation job %T is not supported by the workflow runtime", job.Variant)
}

func (e *workflowExecution) drive() error {
	if e.terminal {
		return nil
	}
	for {
		progressed := false
		for _, coroutine := range e.dispatcher.coroutines {
			if coroutine.done || (coroutine.waiting != nil && !coroutine.waiting.ready) {
				continue
			}
			coroutine.waiting = nil
			e.dispatcher.current = coroutine
			_, ok := coroutine.next()
			e.dispatcher.current = nil
			progressed = true
			if !ok {
				coroutine.done = true
			}
			if coroutine.err != nil {
				e.failWorkflow(coroutine.err)
			}
			if e.terminal {
				return nil
			}
		}
		if !progressed {
			return nil
		}
	}
}

func (e *workflowExecution) nextSeq() uint32 {
	e.nextSequence++
	return e.nextSequence
}

func (e *workflowExecution) emit(command *workflowcommands.WorkflowCommand) {
	e.commands = append(e.commands, command)
}

func (e *workflowExecution) failWorkflow(err error) {
	if e.terminal {
		return
	}
	e.emit(&workflowcommands.WorkflowCommand{
		Variant: &workflowcommands.WorkflowCommand_FailWorkflowExecution{
			FailWorkflowExecution: &workflowcommands.FailWorkflowExecution{
				Failure: newFailure(err, "GoWasmWorkflowError", ""),
			},
		},
	})
	e.terminal = true
}

func (e *workflowExecution) stop() {
	if e.stopped {
		return
	}
	e.stopped = true
	for _, coroutine := range e.dispatcher.coroutines {
		if coroutine.stopPull != nil {
			coroutine.stopPull()
		}
	}
}

func activityResolution(resolution *activityresult.ActivityResolution) (*commonpb.Payload, error) {
	if resolution == nil {
		return nil, errors.New("activity resolution is missing a result")
	}
	if completed := resolution.GetCompleted(); completed != nil {
		return completed.Result, nil
	}
	if failed := resolution.GetFailed(); failed != nil {
		return nil, fmt.Errorf("activity failed: %s", failureMessage(failed.Failure))
	}
	if cancelled := resolution.GetCancelled(); cancelled != nil {
		return nil, fmt.Errorf("activity cancelled: %s", failureMessage(cancelled.Failure))
	}
	if resolution.GetBackoff() != nil {
		return nil, errors.New("local-activity backoff is not supported")
	}
	return nil, errors.New("activity resolution has no status")
}

func childWorkflowResolution(resolution *childworkflow.ChildWorkflowResult) (*commonpb.Payload, error) {
	if resolution == nil {
		return nil, errors.New("child workflow resolution is missing a result")
	}
	if completed := resolution.GetCompleted(); completed != nil {
		return completed.Result, nil
	}
	if failed := resolution.GetFailed(); failed != nil {
		return nil, fmt.Errorf("child workflow failed: %s", failureMessage(failed.Failure))
	}
	if cancelled := resolution.GetCancelled(); cancelled != nil {
		return nil, fmt.Errorf("child workflow cancelled: %s", failureMessage(cancelled.Failure))
	}
	return nil, errors.New("child workflow resolution has no status")
}

func instantiateWorkflowCoreWorker(
	address, namespace, taskQueue, identity string,
	options workflowCoreOptions,
) (coreWorker, error) {
	return corebridge.NewWorkflow(address, namespace, taskQueue, identity, corebridge.WorkflowOptions{
		MaxConcurrentWorkflowTaskExecutions: options.maxConcurrentWorkflowTaskExecutions,
		MaxConcurrentWorkflowTaskPollers:    options.maxConcurrentWorkflowTaskPollers,
		MaxCachedWorkflows:                  options.maxCachedWorkflows,
		Connection: corebridge.ConnectionOptions{
			TLS:    options.tls,
			APIKey: options.apiKey,
		},
	})
}

func normalizeWorkflowOptions(options Options) (workflowCoreOptions, error) {
	normalized := workflowCoreOptions{
		maxConcurrentWorkflowTaskExecutions: int32(options.MaxConcurrentWorkflowTaskExecutionSize),
		maxConcurrentWorkflowTaskPollers:    int32(options.MaxConcurrentWorkflowTaskPollers),
		maxCachedWorkflows:                  int32(options.MaxCachedWorkflows),
		tls:                                 options.TLS,
		apiKey:                              options.APIKey,
	}
	if options.MaxConcurrentWorkflowTaskExecutionSize == 0 {
		normalized.maxConcurrentWorkflowTaskExecutions = defaultMaxConcurrentWorkflowTaskExecutions
	}
	if options.MaxCachedWorkflows == 0 {
		normalized.maxCachedWorkflows = defaultMaxCachedWorkflows
	}

	if options.MaxConcurrentWorkflowTaskExecutionSize < 0 || options.MaxConcurrentWorkflowTaskExecutionSize > math.MaxInt32 {
		return workflowCoreOptions{}, errors.New(
			"maximum concurrent workflow task executions must be between 1 and 2147483647",
		)
	}
	if options.MaxConcurrentWorkflowTaskPollers < 0 || options.MaxConcurrentWorkflowTaskPollers > math.MaxInt32 {
		return workflowCoreOptions{}, errors.New(
			"maximum concurrent workflow task pollers must be between 0 and 2147483647",
		)
	}
	if options.MaxCachedWorkflows < 0 || options.MaxCachedWorkflows > math.MaxInt32 {
		return workflowCoreOptions{}, errors.New("maximum cached workflows must be between 1 and 2147483647")
	}
	if normalized.maxConcurrentWorkflowTaskExecutions < 1 {
		return workflowCoreOptions{}, errors.New(
			"maximum concurrent workflow task executions must be between 1 and 2147483647",
		)
	}
	if normalized.maxCachedWorkflows < 1 {
		return workflowCoreOptions{}, errors.New("maximum cached workflows must be between 1 and 2147483647")
	}
	if normalized.maxCachedWorkflows > 0 && normalized.maxConcurrentWorkflowTaskExecutions < 2 {
		return workflowCoreOptions{}, errors.New(
			"maximum concurrent workflow task executions must be at least 2 when cached workflows are enabled",
		)
	}
	if normalized.maxCachedWorkflows > 0 && normalized.maxConcurrentWorkflowTaskPollers > 0 && normalized.maxConcurrentWorkflowTaskPollers < 2 {
		return workflowCoreOptions{}, errors.New(
			"maximum concurrent workflow task pollers must be at least 2 when cached workflows are enabled",
		)
	}
	return normalized, nil
}

func (w *Worker) pollWorkflows(ctx context.Context, events chan<- workflowPollEvent) {
	defer close(events)
	for {
		encoded, err := w.core.PollWorkflow(ctx)
		event := workflowPollEvent{err: err}
		if err == nil {
			event.activation = &workflowactivation.WorkflowActivation{}
			if err := proto.Unmarshal(encoded, event.activation); err != nil {
				event.activation = nil
				event.err = fmt.Errorf("decode Core workflow activation: %w", err)
			} else if err := converter.DecodePayloads(event.activation, w.codec()); err != nil {
				event.payloadCodecErr = fmt.Errorf("decode workflow activation payloads: %w", err)
			}
		}
		if event.err == nil {
			// Once Core has returned an activation, it is accepted and must receive
			// exactly one completion attempt even if cancellation races this send.
			events <- event
		} else {
			select {
			case events <- event:
			case <-ctx.Done():
				return
			}
		}
		if event.err != nil {
			return
		}
	}
}

func (w *Worker) processActivation(
	activation *workflowactivation.WorkflowActivation,
	runTurn *workflowRunTurn,
	results chan<- workflowProcessorResult,
) {
	if activation.RunId == "" {
		results <- workflowProcessorResult{
			completion: failedActivation("", errors.New("workflow activation is missing a run ID")),
		}
		return
	}
	runTurn.wait()
	completion := func() (completion *workflowcompletion.WorkflowActivationCompletion) {
		defer func() {
			if recovered := recover(); recovered != nil {
				completion = failedActivation(
					activation.RunId,
					fmt.Errorf("workflow activation for run %q panicked: %v\n%s", activation.RunId, recovered, debug.Stack()),
				)
			}
		}()
		return w.handleActivationLocked(activation)
	}()
	results <- workflowProcessorResult{
		completion: completion,
		runID:      activation.RunId,
		runTurn:    runTurn,
	}
}

func (w *Worker) enqueueWorkflowCompletion(
	pipeline *completionpipe.Pipeline,
	completion *workflowcompletion.WorkflowActivationCompletion,
	runID string,
	runTurn *workflowRunTurn,
) error {
	if completion == nil {
		if runTurn != nil {
			w.releaseRunTurn(runID, runTurn)
		}
		return errors.New("workflow activation produced no completion")
	}
	if err := converter.EncodePayloads(completion, w.codec()); err != nil {
		completion = failedActivation(
			completion.RunId,
			fmt.Errorf("encode workflow activation completion payloads: %w", err),
		)
	}
	err := pipeline.Enqueue(completionpipe.Ticket{
		Weight: proto.Size(completion),
		Submit: func() error {
			return w.submitCompletion(completion)
		},
		AfterSubmit: func() {
			if runTurn != nil {
				w.releaseRunTurn(runID, runTurn)
			}
		},
	})
	if err != nil && runTurn != nil {
		w.releaseRunTurn(runID, runTurn)
	}
	return err
}

func (w *Worker) reserveRunTurn(runID string) *workflowRunTurn {
	w.runsMu.Lock()
	defer w.runsMu.Unlock()
	if w.runLocks == nil {
		w.runLocks = make(map[string]*workflowRunLane)
	}
	runLane := w.runLocks[runID]
	if runLane == nil {
		runLane = &workflowRunLane{}
		w.runLocks[runID] = runLane
	}
	runTurn := &workflowRunTurn{
		lane:  runLane,
		ready: runLane.tail,
		done:  make(chan struct{}),
	}
	runLane.tail = runTurn.done
	runLane.refs++
	return runTurn
}

func (turn *workflowRunTurn) wait() {
	if turn.ready != nil {
		<-turn.ready
	}
}

func (w *Worker) releaseRunTurn(runID string, runTurn *workflowRunTurn) {
	close(runTurn.done)
	w.runsMu.Lock()
	defer w.runsMu.Unlock()
	runLane := runTurn.lane
	runLane.refs--
	if runLane.refs == 0 && w.runs[runID] == nil && w.runLocks[runID] == runLane {
		delete(w.runLocks, runID)
	}
}

func (w *Worker) getRun(runID string) *workflowExecution {
	w.runsMu.Lock()
	defer w.runsMu.Unlock()
	return w.runs[runID]
}

func (w *Worker) setRun(runID string, execution *workflowExecution) {
	w.runsMu.Lock()
	defer w.runsMu.Unlock()
	if w.runs == nil {
		w.runs = make(map[string]*workflowExecution)
	}
	w.runs[runID] = execution
}

func (w *Worker) deleteRun(runID string) {
	w.runsMu.Lock()
	defer w.runsMu.Unlock()
	delete(w.runs, runID)
}

func (w *Worker) stopAllRuns() {
	w.runsMu.Lock()
	runs := w.runs
	w.runs = make(map[string]*workflowExecution)
	w.runLocks = make(map[string]*workflowRunLane)
	w.runsMu.Unlock()

	for _, execution := range runs {
		execution.stop()
	}
}

func (w *Worker) shutdownCore() error {
	w.shutdownOnce.Do(func() {
		if w.core != nil {
			w.shutdownErr = w.core.Shutdown()
		}
	})
	return w.shutdownErr
}

func (w *Worker) workflowCompletionLimit() int {
	if w.maxConcurrentWorkflowTaskExecutions > 0 {
		return w.maxConcurrentWorkflowTaskExecutions
	}
	return defaultMaxConcurrentWorkflowTaskExecutions
}

func (w *Worker) submitCompletion(completion *workflowcompletion.WorkflowActivationCompletion) error {
	completionBytes, err := proto.Marshal(completion)
	if err == nil {
		err = w.core.CompleteWorkflow(completionBytes)
	}
	if err != nil {
		err = fmt.Errorf("submit workflow activation completion for run %q: %w", completion.RunId, err)
	}
	return err
}

func (w *Worker) codec() converter.PayloadCodec {
	if w.payloadCodec != nil {
		return w.payloadCodec
	}
	return converter.NewNoopPayloadCodec()
}

func isWorkflowCancellationError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
