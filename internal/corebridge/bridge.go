package corebridge

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	wasmbridgepb "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/wasmbridge"
	corewasm "github.com/temporalio/sdk-go-on-wasm-core/internal/corewasm"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/wasmhost"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	initialOutputCapacity  = 64 * 1024
	maxBridgeMessageSize   = 64 * 1024 * 1024
	errorScratchCapacity   = 64 * 1024
	bridgePending          = int32(1)
	bridgeBufferTooSmall   = int32(2)
	maxHostTransportBytes  = 4 * 1024 * 1024
	maxConcurrentGRPCCalls = 64
	workerModeActivity     = int32(0)
	workerModeWorkflow     = int32(1)
	workerModeCombined     = int32(2)
	bridgeTimerFallback    = 50 * time.Millisecond
	defaultMaxEagerPerWFT  = int32(3)
	workflowServiceName    = "temporal.api.workflowservice.v1.WorkflowService"
	respondWorkflowTaskRPC = "RespondWorkflowTaskCompleted"
)

var errBridgeShuttingDown = errors.New("temporal Core bridge is shutting down")

type bridgeTimingHookFunc func(phase string, elapsed time.Duration)
type eagerActivityDispatchHookFunc func(count int)

var bridgeTimingHook atomic.Pointer[bridgeTimingHookFunc]
var eagerActivityDispatchHook atomic.Pointer[eagerActivityDispatchHookFunc]

// SetTimingHook installs an optional bridge timing hook and returns a restore function.
func SetTimingHook(hook func(phase string, elapsed time.Duration)) func() {
	var installed *bridgeTimingHookFunc
	if hook != nil {
		typedHook := bridgeTimingHookFunc(hook)
		installed = &typedHook
	}
	previous := bridgeTimingHook.Swap(installed)
	return func() {
		bridgeTimingHook.CompareAndSwap(installed, previous)
	}
}

func recordBridgeTiming(phase string, elapsed time.Duration) {
	hook := bridgeTimingHook.Load()
	if hook == nil || *hook == nil {
		return
	}
	(*hook)(phase, elapsed)
}

func bridgeTimingEnabled() bool {
	return bridgeTimingHook.Load() != nil
}

// SetEagerActivityDispatchHook installs an optional hook that reports activity tasks returned
// eagerly with a workflow-task completion. It returns a function that restores the previous hook.
func SetEagerActivityDispatchHook(hook func(count int)) func() {
	var installed *eagerActivityDispatchHookFunc
	if hook != nil {
		typedHook := eagerActivityDispatchHookFunc(hook)
		installed = &typedHook
	}
	previous := eagerActivityDispatchHook.Swap(installed)
	return func() {
		eagerActivityDispatchHook.CompareAndSwap(installed, previous)
	}
}

type Bridge struct {
	module                *corewasm.Module
	host                  *wasmhost.RawGRPCHost
	grpcResponses         chan grpcResponse
	ownerRequests         chan ownerRequest
	doneCtx               context.Context
	cancelDone            context.CancelCauseFunc
	grpcCtx               context.Context
	cancelGRPC            context.CancelFunc
	shutdownOnce          sync.Once
	shutdownErr           error
	completionOperationID atomic.Uint64
	closing               atomic.Bool
	wakeMu                sync.Mutex
	wakeCh                chan struct{}
}

type WorkflowOptions struct {
	MaxConcurrentWorkflowTaskExecutions         int32
	MaxConcurrentWorkflowTaskPollers            int32
	MaxCachedWorkflows                          int32
	MaxEagerActivityReservationsPerWorkflowTask int32
	// WorkerHeartbeatIntervalMillis sets how often Core reports worker status to the
	// server. Zero disables worker heartbeating. Core rejects any other value outside
	// its supported 1s-60s range.
	WorkerHeartbeatIntervalMillis int32
	Connection                    ConnectionOptions
}

type ConnectionOptions struct {
	TLS    *tls.Config
	APIKey string
}

type grpcRequest struct {
	id       uint64
	service  string
	rpc      string
	metadata metadata.MD
	proto    []byte
}

type grpcResponse struct {
	id       uint64
	response []byte
	err      error
	readyAt  time.Time
}

type completionStartFunc func(int64, int32, int32, int32, int32) int64

type completionTakeFunc func(int64, int32, int32) int64

type completionDriver struct {
	phase string
	start completionStartFunc
	take  completionTakeFunc
}

type ownerRequest struct {
	ctx         context.Context
	allowClosed bool
	run         func(*bridgeOwner) (any, error)
	reply       chan ownerResponse
}

type ownerResponse struct {
	value any
	err   error
}

type bridgeOwner struct {
	bridge                 *Bridge
	activityPollInProgress bool
	workflowPollInProgress bool
	grpcCallsInFlight      int
	outputScratch          bridgeBuffer
	errorScratch           bridgeBuffer
}

type bridgeBuffer struct {
	ptr      int32
	capacity int32
}

type operationStepResult struct {
	result []byte
	ready  bool
}

func New(
	address, namespace, taskQueue, identity string,
	maxConcurrentActivityExecutions, maxConcurrentActivityTaskPollers int32,
) (*Bridge, error) {
	return NewWithConnectionOptions(
		address,
		namespace,
		taskQueue,
		identity,
		maxConcurrentActivityExecutions,
		maxConcurrentActivityTaskPollers,
		ConnectionOptions{},
	)
}

func NewWithConnectionOptions(
	address, namespace, taskQueue, identity string,
	maxConcurrentActivityExecutions, maxConcurrentActivityTaskPollers int32,
	connectionOptions ConnectionOptions,
) (*Bridge, error) {
	return newBridge(
		address,
		namespace,
		taskQueue,
		identity,
		maxConcurrentActivityExecutions,
		maxConcurrentActivityTaskPollers,
		WorkflowOptions{},
		workerModeActivity,
		connectionOptions,
	)
}

func NewWorkflow(
	address, namespace, taskQueue, identity string,
	options WorkflowOptions,
) (*Bridge, error) {
	return newBridge(
		address,
		namespace,
		taskQueue,
		identity,
		0,
		0,
		options,
		workerModeWorkflow,
		options.Connection,
	)
}

func NewCombined(
	address, namespace, taskQueue, identity string,
	maxConcurrentActivityExecutions, maxConcurrentActivityTaskPollers int32,
	options WorkflowOptions,
) (*Bridge, error) {
	return newBridge(
		address,
		namespace,
		taskQueue,
		identity,
		maxConcurrentActivityExecutions,
		maxConcurrentActivityTaskPollers,
		options,
		workerModeCombined,
		options.Connection,
	)
}

func newBridge(
	address, namespace, taskQueue, identity string,
	maxConcurrentActivityExecutions, maxConcurrentActivityTaskPollers int32,
	workflowOptions WorkflowOptions,
	workerMode int32,
	connectionOptions ConnectionOptions,
) (*Bridge, error) {
	host, err := wasmhost.NewRawGRPCHostWithOptions(address, wasmhost.GRPCOptions{
		TLS:    connectionOptions.TLS,
		APIKey: connectionOptions.APIKey,
	})
	if err != nil {
		return nil, err
	}
	wasi := wasmhost.NewWASIHost()
	doneCtx, cancelDone := context.WithCancelCause(context.Background())
	grpcCtx, cancelGRPC := context.WithCancel(context.Background())
	bridge := &Bridge{
		host:          host,
		grpcResponses: make(chan grpcResponse, 16),
		ownerRequests: make(chan ownerRequest),
		doneCtx:       doneCtx,
		cancelDone:    cancelDone,
		grpcCtx:       grpcCtx,
		cancelGRPC:    cancelGRPC,
		wakeCh:        make(chan struct{}),
	}
	bridge.module = corewasm.New(wasi)
	go bridge.ownerLoop()
	if err := bridge.init(
		namespace,
		taskQueue,
		identity,
		maxConcurrentActivityExecutions,
		maxConcurrentActivityTaskPollers,
		workflowOptions,
		workerMode,
	); err != nil {
		if context.Cause(bridge.doneCtx) == nil {
			_, releaseErr := bridge.ownerCall(context.Background(), true, func(owner *bridgeOwner) (any, error) {
				owner.releaseScratch()
				return nil, nil
			})
			err = errors.Join(err, releaseErr)
		}
		bridge.cancelDone(err)
		bridge.signalWake()
		_ = host.Close()
		return nil, err
	}
	return bridge, nil
}

func (b *Bridge) PollActivity(ctx context.Context) ([]byte, error) {
	if err := b.StartPollActivity(); err != nil {
		return nil, err
	}
	for {
		wake := b.currentWake()
		if err := b.pollContextErr(ctx); err != nil {
			return nil, err
		}
		result, ready, err := b.PollActivityStep()
		if err != nil || ready {
			return result, err
		}
		if err := b.waitForWake(ctx, wake, false); err != nil {
			return nil, err
		}
	}
}

func (b *Bridge) StartPollActivity() error {
	_, err := b.ownerCall(context.Background(), false, func(owner *bridgeOwner) (any, error) {
		return nil, owner.startPollActivity()
	})
	return err
}

func (b *Bridge) PollActivityStep() ([]byte, bool, error) {
	value, err := b.ownerCall(context.Background(), false, func(owner *bridgeOwner) (any, error) {
		result, ready, runErr := owner.pollActivityStep()
		return operationStepResult{result: result, ready: ready}, runErr
	})
	if err != nil {
		return nil, false, err
	}
	step := value.(operationStepResult)
	return step.result, step.ready, nil
}

func (b *Bridge) PollWorkflow(ctx context.Context) ([]byte, error) {
	if err := b.StartPollWorkflow(); err != nil {
		return nil, err
	}
	for {
		wake := b.currentWake()
		if err := b.pollContextErr(ctx); err != nil {
			return nil, err
		}
		result, ready, err := b.PollWorkflowStep()
		if err != nil || ready {
			return result, err
		}
		if err := b.waitForWake(ctx, wake, false); err != nil {
			return nil, err
		}
	}
}

func (b *Bridge) StartPollWorkflow() error {
	_, err := b.ownerCall(context.Background(), false, func(owner *bridgeOwner) (any, error) {
		return nil, owner.startPollWorkflow()
	})
	return err
}

func (b *Bridge) PollWorkflowStep() ([]byte, bool, error) {
	value, err := b.ownerCall(context.Background(), false, func(owner *bridgeOwner) (any, error) {
		result, ready, runErr := owner.pollWorkflowStep()
		return operationStepResult{result: result, ready: ready}, runErr
	})
	if err != nil {
		return nil, false, err
	}
	step := value.(operationStepResult)
	return step.result, step.ready, nil
}

func (b *Bridge) CompleteActivity(completion []byte) error {
	return b.completeWithDriver(completion, completionDriver{
		phase: "complete_activity",
		start: b.module.Xtemporal_core_start_complete_activity_with_id,
		take:  b.module.Xtemporal_core_take_complete_activity_with_id,
	})
}

func (b *Bridge) CompleteWorkflow(completion []byte) error {
	return b.completeWithDriver(completion, completionDriver{
		phase: "complete_workflow",
		start: b.module.Xtemporal_core_start_complete_workflow_activation_with_id,
		take:  b.module.Xtemporal_core_take_complete_workflow_activation_with_id,
	})
}

func (b *Bridge) nextCompletionOperationID() (uint64, error) {
	for {
		current := b.completionOperationID.Load()
		if current == ^uint64(0) {
			return 0, errors.New("completion operation IDs exhausted")
		}
		if b.completionOperationID.CompareAndSwap(current, current+1) {
			return current + 1, nil
		}
	}
}

func driveCompletion(
	ctx context.Context,
	start func() error,
	take func() (bool, error),
) error {
	if err := start(); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ready, err := take()
		if err != nil || ready {
			return err
		}
	}
}

func (b *Bridge) completeWithDriver(completion []byte, driver completionDriver) error {
	if bridgeTimingEnabled() {
		start := time.Now()
		defer func() { recordBridgeTiming(driver.phase, time.Since(start)) }()
	}
	operationID, err := b.nextCompletionOperationID()
	if err != nil {
		return err
	}
	return driveCompletion(
		context.Background(),
		func() error { return b.startCompletion(operationID, completion, driver.start) },
		func() (bool, error) {
			wake := b.currentWake()
			ready, err := b.takeCompletion(operationID, driver.take)
			if err == nil && !ready {
				err = b.waitForWake(context.Background(), wake, false)
			}
			return ready, err
		},
	)
}

func (b *Bridge) startCompletion(operationID uint64, completion []byte, start completionStartFunc) error {
	_, err := b.ownerCall(context.Background(), false, func(owner *bridgeOwner) (any, error) {
		return nil, owner.startCompletion(operationID, completion, start)
	})
	return err
}

func (b *Bridge) takeCompletion(operationID uint64, take completionTakeFunc) (bool, error) {
	value, err := b.ownerCall(context.Background(), false, func(owner *bridgeOwner) (any, error) {
		_, ready, runErr := owner.operationStep(func(output, capacity int32) int64 {
			return take(int64(operationID), output, capacity)
		})
		return ready, runErr
	})
	if err != nil {
		return false, err
	}
	return value.(bool), nil
}

func (b *Bridge) RecordActivityHeartbeat(heartbeat []byte) error {
	_, err := b.ownerCall(context.Background(), false, func(owner *bridgeOwner) (any, error) {
		return nil, owner.recordActivityHeartbeat(heartbeat)
	})
	return err
}

func (b *Bridge) Shutdown() error {
	b.shutdownOnce.Do(func() {
		b.shutdownErr = b.shutdown()
	})
	return b.shutdownErr
}

func (b *Bridge) shutdown() error {
	b.closing.Store(true)
	b.cancelGRPC()
	b.signalWake()
	_, err := b.ownerCall(context.Background(), true, func(owner *bridgeOwner) (any, error) {
		return nil, owner.callForError(b.module.Xtemporal_core_start_shutdown)
	})
	if err == nil {
		_, err = b.drive(context.Background(), true, func(owner *bridgeOwner) ([]byte, bool, error) {
			return owner.operationStep(b.module.Xtemporal_core_take_shutdown)
		})
	}
	var releaseErr error
	if context.Cause(b.doneCtx) == nil {
		_, releaseErr = b.ownerCall(context.Background(), true, func(owner *bridgeOwner) (any, error) {
			owner.releaseScratch()
			return nil, nil
		})
	}
	closeErr := b.host.Close()
	resultErr := errors.Join(err, releaseErr, closeErr)
	cause := resultErr
	if cause == nil {
		cause = errBridgeShuttingDown
	}
	b.cancelDone(cause)
	b.signalWake()
	return resultErr
}

func (b *Bridge) drive(
	ctx context.Context,
	allowClosed bool,
	step func(*bridgeOwner) ([]byte, bool, error),
) ([]byte, error) {
	for {
		wake := b.currentWake()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !allowClosed {
			if err := b.operationErr(); err != nil {
				return nil, err
			}
		}
		value, err := b.ownerCall(ctx, allowClosed, func(owner *bridgeOwner) (any, error) {
			result, ready, runErr := step(owner)
			return operationStepResult{result: result, ready: ready}, runErr
		})
		if err != nil {
			return nil, err
		}
		result := value.(operationStepResult)
		if result.ready {
			return result.result, nil
		}
		if err := b.waitForWake(ctx, wake, allowClosed); err != nil {
			return nil, err
		}
	}
}

func (b *Bridge) ownerCall(
	ctx context.Context,
	allowClosed bool,
	run func(*bridgeOwner) (any, error),
) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !allowClosed {
		if err := b.operationErr(); err != nil {
			return nil, err
		}
	}
	reply := make(chan ownerResponse, 1)
	request := ownerRequest{
		ctx:         ctx,
		allowClosed: allowClosed,
		run:         run,
		reply:       reply,
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.doneCtx.Done():
		return nil, b.closedErr()
	case b.ownerRequests <- request:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case response := <-reply:
		return response.value, response.err
	}
}

func (b *Bridge) ownerLoop() {
	owner := &bridgeOwner{bridge: b}
	for {
		select {
		case request := <-b.ownerRequests:
			if err := request.ctx.Err(); err != nil {
				request.reply <- ownerResponse{err: err}
				continue
			}
			if !request.allowClosed {
				if err := b.operationErr(); err != nil {
					request.reply <- ownerResponse{err: err}
					continue
				}
			} else if err := context.Cause(b.doneCtx); err != nil && !errors.Is(err, errBridgeShuttingDown) {
				request.reply <- ownerResponse{err: err}
				continue
			}
			request.reply <- b.runOwnerRequest(owner, request.run)
		case <-b.doneCtx.Done():
			for {
				select {
				case request := <-b.ownerRequests:
					request.reply <- ownerResponse{err: b.closedErr()}
				default:
					return
				}
			}
		}
	}
}

func (b *Bridge) runOwnerRequest(
	owner *bridgeOwner,
	run func(*bridgeOwner) (any, error),
) (response ownerResponse) {
	defer func() {
		if recovered := recover(); recovered != nil {
			response.err = owner.poison(newBridgePanicError(recovered))
		}
	}()
	response.value, response.err = run(owner)
	return response
}

func newBridgePanicError(recovered any) error {
	return fmt.Errorf("temporal Core bridge panicked: %v\n%s", recovered, debug.Stack())
}

func (b *Bridge) currentWake() <-chan struct{} {
	b.wakeMu.Lock()
	defer b.wakeMu.Unlock()
	return b.wakeCh
}

func (b *Bridge) signalWake() {
	b.wakeMu.Lock()
	close(b.wakeCh)
	b.wakeCh = make(chan struct{})
	b.wakeMu.Unlock()
}

func (b *Bridge) waitForWake(ctx context.Context, wake <-chan struct{}, allowClosed bool) error {
	if bridgeTimingEnabled() {
		start := time.Now()
		defer func() { recordBridgeTiming("wait_for_wake", time.Since(start)) }()
	}
	timer := time.NewTimer(bridgeTimerFallback)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
	case <-timer.C:
	case <-b.doneCtx.Done():
		if err := context.Cause(b.doneCtx); err != nil {
			if !allowClosed || !errors.Is(err, errBridgeShuttingDown) {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !allowClosed {
		if err := b.operationErr(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bridge) pollContextErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.operationErr()
}

func (b *Bridge) operationErr() error {
	if err := context.Cause(b.doneCtx); err != nil {
		return err
	}
	if b.closing.Load() {
		return errBridgeShuttingDown
	}
	return nil
}

func (b *Bridge) closedErr() error {
	if err := context.Cause(b.doneCtx); err != nil {
		return err
	}
	if b.closing.Load() {
		return errBridgeShuttingDown
	}
	return errors.New("temporal Core bridge is closed")
}

func (b *Bridge) init(
	namespace, taskQueue, identity string,
	maxConcurrentActivityExecutions, maxConcurrentActivityTaskPollers int32,
	workflowOptions WorkflowOptions,
	workerMode int32,
) error {
	_, err := b.ownerCall(context.Background(), false, func(owner *bridgeOwner) (any, error) {
		return nil, owner.startInit(
			namespace,
			taskQueue,
			identity,
			maxConcurrentActivityExecutions,
			maxConcurrentActivityTaskPollers,
			workflowOptions,
			workerMode,
		)
	})
	if err != nil {
		return err
	}
	_, err = b.drive(context.Background(), false, func(owner *bridgeOwner) ([]byte, bool, error) {
		return owner.operationStep(b.module.Xtemporal_core_take_init)
	})
	return err
}

func (b *Bridge) callGRPC(ctx context.Context, request grpcRequest) grpcResponse {
	response := b.host.CallContext(ctx, wasmhost.GRPCRequest{
		Service:  request.service,
		RPC:      request.rpc,
		Metadata: request.metadata,
		Proto:    request.proto,
	})
	recordEagerActivityDispatch(request, response)
	encoded, err := encodeGRPCResponse(response)
	if err != nil {
		response = wasmhost.GRPCResponse{Code: codes.Internal, Message: fmt.Sprintf("encode gRPC response: %v", err)}
		encoded, err = encodeGRPCResponse(response)
	}
	return grpcResponse{id: request.id, response: encoded, err: err}
}

func (b *Bridge) deliverGRPCResponse(response grpcResponse) bool {
	select {
	case <-b.doneCtx.Done():
		return false
	case b.grpcResponses <- response:
		b.signalWake()
		return true
	}
}

func (o *bridgeOwner) poison(cause error) error {
	o.bridge.closing.Store(true)
	o.bridge.cancelGRPC()
	closeErr := error(nil)
	if o.bridge.host != nil {
		closeErr = o.bridge.host.Close()
	}
	poisonErr := errors.Join(cause, closeErr)
	if poisonErr == nil {
		poisonErr = cause
	}
	o.bridge.cancelDone(poisonErr)
	o.bridge.signalWake()
	return poisonErr
}

func (o *bridgeOwner) startPollActivity() error {
	if o.activityPollInProgress {
		return nil
	}
	if err := o.callForError(o.bridge.module.Xtemporal_core_start_poll_activity); err != nil {
		return err
	}
	o.activityPollInProgress = true
	return nil
}

func (o *bridgeOwner) pollActivityStep() ([]byte, bool, error) {
	if !o.activityPollInProgress {
		return nil, false, errors.New("activity poll is not in progress")
	}
	result, ready, err := o.operationStep(o.bridge.module.Xtemporal_core_take_poll_activity)
	if ready || err != nil {
		o.activityPollInProgress = false
	}
	return result, ready, err
}

func (o *bridgeOwner) startPollWorkflow() error {
	if o.workflowPollInProgress {
		return nil
	}
	if err := o.callForError(o.bridge.module.Xtemporal_core_start_poll_workflow_activation); err != nil {
		return err
	}
	o.workflowPollInProgress = true
	return nil
}

func (o *bridgeOwner) pollWorkflowStep() ([]byte, bool, error) {
	if !o.workflowPollInProgress {
		return nil, false, errors.New("workflow poll is not in progress")
	}
	result, ready, err := o.operationStep(o.bridge.module.Xtemporal_core_take_poll_workflow_activation)
	if ready || err != nil {
		o.workflowPollInProgress = false
	}
	return result, ready, err
}

func (o *bridgeOwner) startCompletion(
	operationID uint64,
	completion []byte,
	start completionStartFunc,
) error {
	completionLen, err := checkedBridgeInputLen(len(completion))
	if err != nil {
		return fmt.Errorf("completion length: %w", err)
	}
	input, err := o.copyIn(completion)
	if err != nil {
		return err
	}
	defer o.bridge.module.Xtemporal_dealloc(input, completionLen)
	errorOutput, err := o.ensureErrorScratch()
	if err != nil {
		return err
	}
	_, err = o.requireSuccess(
		start(
			int64(operationID),
			input,
			completionLen,
			errorOutput.ptr,
			errorOutput.capacity,
		),
		errorOutput,
	)
	return err
}

func (o *bridgeOwner) recordActivityHeartbeat(heartbeat []byte) error {
	heartbeatLen, err := checkedBridgeInputLen(len(heartbeat))
	if err != nil {
		return fmt.Errorf("heartbeat length: %w", err)
	}
	input, err := o.copyIn(heartbeat)
	if err != nil {
		return err
	}
	defer o.bridge.module.Xtemporal_dealloc(input, heartbeatLen)
	errorOutput, err := o.ensureErrorScratch()
	if err != nil {
		return err
	}
	_, err = o.requireSuccess(
		o.bridge.module.Xtemporal_core_record_activity_heartbeat(
			input,
			heartbeatLen,
			errorOutput.ptr,
			errorOutput.capacity,
		),
		errorOutput,
	)
	return err
}

func (o *bridgeOwner) startInit(
	namespace, taskQueue, identity string,
	maxConcurrentActivityExecutions, maxConcurrentActivityTaskPollers int32,
	workflowOptions WorkflowOptions,
	workerMode int32,
) error {
	namespaceBytes := []byte(namespace)
	taskQueueBytes := []byte(taskQueue)
	identityBytes := []byte(identity)
	namespaceLen, err := checkedBridgeInputLen(len(namespaceBytes))
	if err != nil {
		return fmt.Errorf("namespace length: %w", err)
	}
	taskQueueLen, err := checkedBridgeInputLen(len(taskQueueBytes))
	if err != nil {
		return fmt.Errorf("task queue length: %w", err)
	}
	identityLen, err := checkedBridgeInputLen(len(identityBytes))
	if err != nil {
		return fmt.Errorf("identity length: %w", err)
	}
	namespacePtr, err := o.copyIn(namespaceBytes)
	if err != nil {
		return err
	}
	defer o.bridge.module.Xtemporal_dealloc(namespacePtr, namespaceLen)
	taskQueuePtr, err := o.copyIn(taskQueueBytes)
	if err != nil {
		return err
	}
	defer o.bridge.module.Xtemporal_dealloc(taskQueuePtr, taskQueueLen)
	identityPtr, err := o.copyIn(identityBytes)
	if err != nil {
		return err
	}
	defer o.bridge.module.Xtemporal_dealloc(identityPtr, identityLen)
	errorOutput, err := o.ensureErrorScratch()
	if err != nil {
		return err
	}
	maxEagerActivityReservationsPerWorkflowTask, err := eagerActivityReservationsForMode(workerMode, workflowOptions)
	if err != nil {
		return err
	}
	heartbeatIntervalMillis, err := workerHeartbeatIntervalMillis(workflowOptions)
	if err != nil {
		return err
	}
	_, err = o.requireSuccess(
		o.bridge.module.Xtemporal_core_init_with_worker_options(
			namespacePtr, namespaceLen,
			taskQueuePtr, taskQueueLen,
			identityPtr, identityLen,
			maxConcurrentActivityExecutions,
			maxConcurrentActivityTaskPollers,
			workflowOptions.MaxConcurrentWorkflowTaskExecutions,
			workflowOptions.MaxConcurrentWorkflowTaskPollers,
			workflowOptions.MaxCachedWorkflows,
			maxEagerActivityReservationsPerWorkflowTask,
			heartbeatIntervalMillis,
			workerMode,
			errorOutput.ptr,
			errorOutput.capacity,
		),
		errorOutput,
	)
	return err
}

func workerHeartbeatIntervalMillis(workflowOptions WorkflowOptions) (int32, error) {
	if workflowOptions.WorkerHeartbeatIntervalMillis < 0 {
		return 0, errors.New("worker heartbeat interval must be non-negative")
	}
	return workflowOptions.WorkerHeartbeatIntervalMillis, nil
}

func eagerActivityReservationsForMode(workerMode int32, workflowOptions WorkflowOptions) (int32, error) {
	if workerMode != workerModeCombined {
		return 0, nil
	}
	eagerReservations := workflowOptions.MaxEagerActivityReservationsPerWorkflowTask
	if eagerReservations == 0 {
		eagerReservations = defaultMaxEagerPerWFT
	}
	if eagerReservations < 0 {
		return 0, fmt.Errorf(
			"combined worker eager activity reservations per workflow task must be non-negative",
		)
	}
	return eagerReservations, nil
}

func recordEagerActivityDispatch(request grpcRequest, response wasmhost.GRPCResponse) {
	eagerHook := eagerActivityDispatchHook.Load()
	if !bridgeTimingEnabled() && eagerHook == nil {
		return
	}
	if request.service != workflowServiceName ||
		request.rpc != respondWorkflowTaskRPC ||
		response.Code != codes.OK ||
		len(response.Proto) == 0 {
		return
	}
	var completion workflowservicepb.RespondWorkflowTaskCompletedResponse
	if err := proto.Unmarshal(response.Proto, &completion); err != nil {
		return
	}
	activityCount := len(completion.GetActivityTasks())
	if eagerHook != nil && *eagerHook != nil {
		(*eagerHook)(activityCount)
	}
	for range activityCount {
		recordBridgeTiming("eager_activity_dispatch", 0)
	}
}

func (o *bridgeOwner) operationStep(
	takeResult func(int32, int32) int64,
) ([]byte, bool, error) {
	if err := o.ensureOutputScratch(initialOutputCapacity); err != nil {
		return nil, false, err
	}
	return driveOperationStep(
		func() ([]byte, int32, error) {
			if _, err := o.requireSuccess(
				o.bridge.module.Xtemporal_core_drain_ready_tasks(o.outputScratch.ptr, o.outputScratch.capacity),
				o.outputScratch,
			); err != nil {
				return nil, 0, err
			}
			code, result, err := o.takeWithRetry(takeResult)
			return result, code, err
		},
		o.pumpGRPC,
	)
}

func driveOperationStep(
	take func() ([]byte, int32, error),
	pumpGRPC func() (bool, error),
) ([]byte, bool, error) {
	for {
		result, code, err := take()
		if err != nil {
			return nil, false, err
		}
		if code == 0 {
			return result, true, nil
		}
		if code != bridgePending {
			return nil, false, fmt.Errorf("temporal Core bridge error: %s", result)
		}
		progressed, err := pumpGRPC()
		if err != nil {
			return nil, false, err
		}
		if !progressed {
			return nil, false, nil
		}
	}
}

func (o *bridgeOwner) pumpGRPC() (bool, error) {
	progressed := false
	for {
		select {
		case response := <-o.bridge.grpcResponses:
			if o.grpcCallsInFlight == 0 {
				return progressed, errors.New("received a gRPC response with no call in flight")
			}
			o.grpcCallsInFlight--
			if !response.readyAt.IsZero() {
				recordBridgeTiming("grpc_response_queue_delay", time.Since(response.readyAt))
			}
			if response.err != nil {
				return progressed, response.err
			}
			if err := o.completeGRPCRequest(response.id, response.response); err != nil {
				return progressed, err
			}
			progressed = true
		default:
			goto startRequests
		}
	}

startRequests:
	for o.grpcCallsInFlight < maxConcurrentGRPCCalls {
		request, ok, err := o.takeGRPCRequest()
		if err != nil {
			return progressed, err
		}
		if !ok {
			return progressed, nil
		}
		o.grpcCallsInFlight++
		go func(request grpcRequest) {
			response := o.bridge.callGRPC(o.bridge.grpcCtx, request)
			if bridgeTimingEnabled() {
				response.readyAt = time.Now()
			}
			_ = o.bridge.deliverGRPCResponse(response)
		}(request)
	}
	return progressed, nil
}

func (o *bridgeOwner) takeGRPCRequest() (grpcRequest, bool, error) {
	code, encoded, err := o.takeWithRetry(o.bridge.module.Xtemporal_core_take_grpc_request)
	if err != nil {
		return grpcRequest{}, false, err
	}
	if code == bridgePending {
		return grpcRequest{}, false, nil
	}
	if code != 0 {
		return grpcRequest{}, false, fmt.Errorf("temporal Core bridge error: %s", encoded)
	}
	request, err := decodeGRPCRequest(encoded)
	return request, true, err
}

func (o *bridgeOwner) completeGRPCRequest(id uint64, response []byte) error {
	if bridgeTimingEnabled() {
		start := time.Now()
		defer func() { recordBridgeTiming("complete_grpc_request", time.Since(start)) }()
	}
	responseLen, err := checkedBridgeInputLen(len(response))
	if err != nil {
		return fmt.Errorf("gRPC response length: %w", err)
	}
	responsePtr, err := o.copyIn(response)
	if err != nil {
		return err
	}
	defer o.bridge.module.Xtemporal_dealloc(responsePtr, responseLen)
	errorOutput, err := o.ensureErrorScratch()
	if err != nil {
		return err
	}
	_, err = o.requireSuccess(
		o.bridge.module.Xtemporal_core_complete_grpc_request(
			int64(id),
			responsePtr,
			responseLen,
			errorOutput.ptr,
			errorOutput.capacity,
		),
		errorOutput,
	)
	return err
}

func (o *bridgeOwner) callForError(call func(int32, int32) int64) error {
	errorOutput, err := o.ensureErrorScratch()
	if err != nil {
		return err
	}
	_, err = o.requireSuccess(call(errorOutput.ptr, errorOutput.capacity), errorOutput)
	return err
}

func (o *bridgeOwner) ensureOutputScratch(minCapacity int32) error {
	if minCapacity <= 0 {
		minCapacity = initialOutputCapacity
	}
	if o.outputScratch.capacity >= minCapacity {
		return nil
	}
	if minCapacity > maxBridgeMessageSize {
		return fmt.Errorf("requested bridge output scratch %d exceeds max %d", minCapacity, maxBridgeMessageSize)
	}
	return o.resizeOutputScratch(minCapacity)
}

func (o *bridgeOwner) growOutputScratch(required uint32) error {
	nextCapacity, err := nextBridgeBufferCapacity(o.outputScratch.capacity, required)
	if err != nil {
		return err
	}
	return o.resizeOutputScratch(nextCapacity)
}

func (o *bridgeOwner) resizeOutputScratch(newCapacity int32) error {
	ptr := o.bridge.module.Xtemporal_alloc(newCapacity)
	if o.outputScratch.capacity != 0 {
		o.bridge.module.Xtemporal_dealloc(o.outputScratch.ptr, o.outputScratch.capacity)
	}
	o.outputScratch = bridgeBuffer{ptr: ptr, capacity: newCapacity}
	return nil
}

func (o *bridgeOwner) ensureErrorScratch() (bridgeBuffer, error) {
	if o.errorScratch.capacity != 0 {
		return o.errorScratch, nil
	}
	o.errorScratch = bridgeBuffer{
		ptr:      o.bridge.module.Xtemporal_alloc(errorScratchCapacity),
		capacity: errorScratchCapacity,
	}
	return o.errorScratch, nil
}

func (o *bridgeOwner) releaseScratch() {
	if o.outputScratch.capacity != 0 {
		o.bridge.module.Xtemporal_dealloc(o.outputScratch.ptr, o.outputScratch.capacity)
		o.outputScratch = bridgeBuffer{}
	}
	if o.errorScratch.capacity != 0 {
		o.bridge.module.Xtemporal_dealloc(o.errorScratch.ptr, o.errorScratch.capacity)
		o.errorScratch = bridgeBuffer{}
	}
}

func (o *bridgeOwner) takeWithRetry(
	takeResult func(int32, int32) int64,
) (int32, []byte, error) {
	if err := o.ensureOutputScratch(initialOutputCapacity); err != nil {
		return 0, nil, err
	}
	for {
		code, output, required, err := o.unpackResult(
			takeResult(o.outputScratch.ptr, o.outputScratch.capacity),
			o.outputScratch,
		)
		if err != nil {
			return 0, nil, err
		}
		if code != bridgeBufferTooSmall {
			return code, output, nil
		}
		if err := o.growOutputScratch(required); err != nil {
			return 0, nil, err
		}
	}
}

func (o *bridgeOwner) copyIn(value []byte) (int32, error) {
	length, err := checkedBridgeInputLen(len(value))
	if err != nil {
		return 0, err
	}
	ptr := o.bridge.module.Xtemporal_alloc(length)
	memory := *o.bridge.module.Xmemory().Slice()
	copy(memory[uint32(ptr):], value)
	return ptr, nil
}

func (o *bridgeOwner) requireSuccess(result int64, output bridgeBuffer) ([]byte, error) {
	code, payload, required, err := o.unpackResult(result, output)
	if err != nil {
		return nil, err
	}
	if code == bridgeBufferTooSmall {
		return nil, fmt.Errorf(
			"temporal Core bridge output requires %d bytes, but non-retryable calls are capped at %d bytes",
			required,
			output.capacity,
		)
	}
	if code != 0 {
		return nil, fmt.Errorf("temporal Core bridge error: %s", payload)
	}
	return payload, nil
}

func (o *bridgeOwner) unpackResult(
	result int64,
	output bridgeBuffer,
) (int32, []byte, uint32, error) {
	word := uint64(result)
	code := int32(word >> 32)
	length := uint32(word)
	if code == bridgeBufferTooSmall {
		return code, nil, length, nil
	}
	memory := *o.bridge.module.Xmemory().Slice()
	start := uint64(uint32(output.ptr))
	end := start + uint64(length)
	if end > uint64(len(memory)) {
		return 0, nil, 0, errors.New("core returned an output range outside WASM memory")
	}
	return code, append([]byte(nil), memory[start:end]...), length, nil
}

func checkedBridgeInputLen(length int) (int32, error) {
	if length < 0 || length > maxBridgeMessageSize {
		return 0, fmt.Errorf("length %d exceeds max bridge message size %d", length, maxBridgeMessageSize)
	}
	return int32(length), nil
}

func nextBridgeBufferCapacity(current int32, required uint32) (int32, error) {
	if required == 0 {
		return 0, errors.New("temporal Core requested a zero-byte bridge buffer resize")
	}
	if required > maxBridgeMessageSize {
		return 0, fmt.Errorf(
			"temporal Core requested %d bytes, exceeding max bridge message size %d",
			required,
			maxBridgeMessageSize,
		)
	}
	capacity := int64(current)
	if capacity < initialOutputCapacity {
		capacity = initialOutputCapacity
	}
	target := int64(required)
	for capacity < target {
		capacity *= 2
		if capacity > maxBridgeMessageSize {
			capacity = maxBridgeMessageSize
			break
		}
	}
	if capacity < target {
		return 0, fmt.Errorf(
			"temporal Core requested %d bytes, exceeding max bridge message size %d",
			required,
			maxBridgeMessageSize,
		)
	}
	return int32(capacity), nil
}

func decodeGRPCRequest(encoded []byte) (grpcRequest, error) {
	if len(encoded) > maxHostTransportBytes {
		return grpcRequest{}, fmt.Errorf("core gRPC request is %d bytes, exceeds %d byte limit", len(encoded), maxHostTransportBytes)
	}
	var wire wasmbridgepb.GrpcRequest
	if err := proto.Unmarshal(encoded, &wire); err != nil {
		return grpcRequest{}, fmt.Errorf("decode core gRPC request: %w", err)
	}
	requestMetadata := metadata.MD{}
	for index, entry := range wire.Metadata {
		if entry == nil {
			return grpcRequest{}, fmt.Errorf("core gRPC request metadata entry %d is missing", index)
		}
		key := strings.ToLower(entry.Key)
		if key == "" {
			return grpcRequest{}, fmt.Errorf("core gRPC request metadata entry %d has an empty key", index)
		}
		requestMetadata.Append(key, string(entry.Value))
	}
	return grpcRequest{
		id:       wire.Id,
		service:  wire.Service,
		rpc:      wire.Rpc,
		metadata: requestMetadata,
		proto:    wire.Proto,
	}, nil
}

func encodeGRPCResponse(response wasmhost.GRPCResponse) ([]byte, error) {
	wire := wasmbridgepb.GrpcResponse{
		StatusCode:    int32(response.Code),
		StatusMessage: response.Message,
		Headers:       grpcMetadataEntries(response.Header),
		Trailers:      grpcMetadataEntries(response.Trailer),
		StatusDetails: response.Details,
		Proto:         response.Proto,
	}
	if size := proto.Size(&wire); size > maxHostTransportBytes {
		return nil, fmt.Errorf("gRPC response is %d bytes, exceeds %d byte limit", size, maxHostTransportBytes)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&wire)
	if err != nil {
		return nil, fmt.Errorf("encode gRPC response: %w", err)
	}
	return encoded, nil
}

func grpcMetadataEntries(md metadata.MD) []*wasmbridgepb.MetadataEntry {
	keys := make([]string, 0, len(md))
	for key := range md {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	entries := make([]*wasmbridgepb.MetadataEntry, 0, len(md))
	for _, key := range keys {
		for _, value := range md[key] {
			entries = append(entries, &wasmbridgepb.MetadataEntry{Key: key, Value: []byte(value)})
		}
	}
	return entries
}
