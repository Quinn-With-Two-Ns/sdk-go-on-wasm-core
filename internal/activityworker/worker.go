package activityworker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"sync"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/completionpipe"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/corebridge"
	corepb "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb"
	activityresult "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/activityresult"
	activitytask "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/activitytask"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/workerconfig"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/proto"
)

type heartbeatRecorder func([]*commonpb.Payload) error

type heartbeatContextKey struct{}
type payloadConverterContextKey struct{}

type activityEvent struct {
	activity   *runningActivity
	completion *corepb.ActivityTaskCompletion
}

type pollEvent struct {
	task            *activitytask.ActivityTask
	payloadCodecErr error
	err             error
}

type runningActivity struct {
	taskToken []byte
	cancel    context.CancelFunc
	done      chan struct{}
	mu        sync.Mutex
	stopped   bool
	terminal  bool
}

type coreWorker interface {
	PollActivity(context.Context) ([]byte, error)
	CompleteActivity([]byte) error
	RecordActivityHeartbeat([]byte) error
	Shutdown() error
}

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
	// MaxConcurrentActivityExecutionSize limits the activity slots managed by Core.
	// The default is 1000.
	MaxConcurrentActivityExecutionSize int
	// MaxConcurrentActivityTaskPollers sets a fixed Core poller maximum. A zero
	// value lets Core select fixed or autoscaling pollers from server capabilities.
	MaxConcurrentActivityTaskPollers int
}

type Worker struct {
	core                            coreWorker
	payloadConverter                converter.PayloadConverter
	payloadCodec                    converter.PayloadCodec
	handlers                        map[string]*registeredActivity
	maxConcurrentActivityExecutions int
}

const defaultMaxConcurrentActivityExecutions = 1000

var newActivityCoreWorker = instantiateActivityCoreWorker

func New(options Options) (*Worker, error) {
	connection, err := workerconfig.Normalize("activity", workerconfig.Connection{
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
	if options.MaxConcurrentActivityExecutionSize == 0 {
		options.MaxConcurrentActivityExecutionSize = defaultMaxConcurrentActivityExecutions
	}
	if options.MaxConcurrentActivityExecutionSize < 0 || options.MaxConcurrentActivityExecutionSize > math.MaxInt32 {
		return nil, errors.New("maximum concurrent activity executions must be between 1 and 2147483647")
	}
	if options.MaxConcurrentActivityTaskPollers < 0 || options.MaxConcurrentActivityTaskPollers > math.MaxInt32 {
		return nil, errors.New("maximum concurrent activity task pollers must be between 0 and 2147483647")
	}
	core, err := newActivityCoreWorker(
		connection.Address,
		connection.Namespace,
		connection.TaskQueue,
		connection.Identity,
		int32(options.MaxConcurrentActivityExecutionSize),
		int32(options.MaxConcurrentActivityTaskPollers),
		connection.TLS,
		connection.APIKey,
	)
	if err != nil {
		return nil, err
	}
	return newWorkerWithCore(connection, options.MaxConcurrentActivityExecutionSize, core), nil
}

// NewWithCore creates an activity worker that reuses an existing Core worker.
func NewWithCore(options Options, core interface {
	PollActivity(context.Context) ([]byte, error)
	CompleteActivity([]byte) error
	RecordActivityHeartbeat([]byte) error
	Shutdown() error
}) (*Worker, error) {
	connection, err := workerconfig.Normalize("activity", workerconfig.Connection{
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
	if options.MaxConcurrentActivityExecutionSize == 0 {
		options.MaxConcurrentActivityExecutionSize = defaultMaxConcurrentActivityExecutions
	}
	if options.MaxConcurrentActivityExecutionSize < 0 || options.MaxConcurrentActivityExecutionSize > math.MaxInt32 {
		return nil, errors.New("maximum concurrent activity executions must be between 1 and 2147483647")
	}
	if options.MaxConcurrentActivityTaskPollers < 0 || options.MaxConcurrentActivityTaskPollers > math.MaxInt32 {
		return nil, errors.New("maximum concurrent activity task pollers must be between 0 and 2147483647")
	}
	return newWorkerWithCore(connection, options.MaxConcurrentActivityExecutionSize, core), nil
}

func newWorkerWithCore(
	connection workerconfig.Connection,
	maxConcurrentActivityExecutions int,
	core coreWorker,
) *Worker {
	return &Worker{
		core:                            core,
		payloadConverter:                connection.PayloadConverter,
		payloadCodec:                    connection.PayloadCodec,
		handlers:                        make(map[string]*registeredActivity),
		maxConcurrentActivityExecutions: maxConcurrentActivityExecutions,
	}
}

func instantiateActivityCoreWorker(
	address, namespace, taskQueue, identity string,
	maxConcurrentActivityExecutions, maxConcurrentActivityTaskPollers int32,
	tlsConfig *tls.Config,
	apiKey string,
) (coreWorker, error) {
	return corebridge.NewWithConnectionOptions(
		address,
		namespace,
		taskQueue,
		identity,
		maxConcurrentActivityExecutions,
		maxConcurrentActivityTaskPollers,
		corebridge.ConnectionOptions{TLS: tlsConfig, APIKey: apiKey},
	)
}

// Register registers an activity function. The function may optionally accept
// context.Context first, may accept any number of data-convertible arguments,
// and must return error or (result, error).
func (w *Worker) Register(name string, function any) {
	if _, exists := w.handlers[name]; exists {
		panic("activity is already registered: " + name)
	}
	activity, err := newRegisteredActivity(name, function)
	if err != nil {
		panic(err)
	}
	w.handlers[name] = activity
}

func (w *Worker) Run(ctx context.Context) (runErr error) {
	pollCtx, stopPolling := context.WithCancel(ctx)
	pollEvents := make(chan pollEvent)
	activityEvents := make(chan activityEvent)
	completionPipeline := completionpipe.New(w.activityCompletionLimit())
	go w.pollActivities(pollCtx, pollEvents)

	defer func() {
		stopPolling()
		runErr = errors.Join(runErr, w.core.Shutdown())
	}()

	return newActivityRunLoop(ctx, w, stopPolling, pollEvents, activityEvents, completionPipeline).run()
}

func (w *Worker) Shutdown() error {
	return w.core.Shutdown()
}

func (w *Worker) pollActivities(ctx context.Context, events chan<- pollEvent) {
	defer close(events)
	for {
		encoded, err := w.core.PollActivity(ctx)
		event := pollEvent{err: err}
		if err == nil {
			event.task = &activitytask.ActivityTask{}
			if err := proto.Unmarshal(encoded, event.task); err != nil {
				event.err = fmt.Errorf("decode Core activity task: %w", err)
			} else if err := converter.DecodePayloads(event.task, w.codec()); err != nil {
				event.payloadCodecErr = fmt.Errorf("decode activity task payloads: %w", err)
			}
		}
		if event.err == nil {
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

func (w *Worker) startActivity(
	ctx context.Context,
	running map[string]*runningActivity,
	activeExecutions *int,
	taskToken []byte,
	start *activitytask.Start,
	events chan<- activityEvent,
) error {
	if *activeExecutions >= w.activityCompletionLimit() {
		return fmt.Errorf(
			"core exceeded the configured maximum of %d concurrent activity executions",
			w.activityCompletionLimit(),
		)
	}
	key := string(taskToken)
	if _, exists := running[string(taskToken)]; exists {
		return errors.New("core delivered a duplicate activity task token")
	}

	activityCtx, cancelActivity := context.WithCancel(ctx)
	activity := &runningActivity{
		taskToken: append([]byte(nil), taskToken...),
		cancel:    cancelActivity,
		done:      make(chan struct{}),
	}
	recorder := heartbeatRecorder(func(details []*commonpb.Payload) error {
		return activity.recordHeartbeat(func() error {
			return w.recordHeartbeat(activity.taskToken, details)
		})
	})
	activityCtx = context.WithValue(activityCtx, heartbeatContextKey{}, recorder)
	activityCtx = context.WithValue(activityCtx, payloadConverterContextKey{}, w.payloadConverterOrDefault())
	running[key] = activity
	*activeExecutions++
	go func() {
		event := activityEvent{
			activity:   activity,
			completion: w.executeSafely(activityCtx, activity.taskToken, start),
		}
		select {
		case events <- event:
		case <-activity.done:
		}
	}()
	return nil
}

func (w *Worker) cancelRunningActivity(
	running map[string]*runningActivity,
	activeExecutions *int,
	taskToken []byte,
	message string,
	pipeline *completionpipe.Pipeline,
) error {
	activity := running[string(taskToken)]
	if activity == nil {
		return nil
	}
	if message == "" {
		message = "activity cancellation requested"
	}
	return w.enqueueActivityCompletion(running, activeExecutions, activity, cancelledCompletion(taskToken, message), pipeline)
}

func (w *Worker) cancelRunningActivities(
	running map[string]*runningActivity,
	activeExecutions *int,
	message string,
	pipeline *completionpipe.Pipeline,
) error {
	var completionErrors []error
	for _, activity := range running {
		if err := w.enqueueActivityCompletion(running, activeExecutions, activity, cancelledCompletion(activity.taskToken, message), pipeline); err != nil {
			completionErrors = append(completionErrors, err)
		}
	}
	return errors.Join(completionErrors...)
}

func (a *runningActivity) queueTerminal() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.terminal {
		return false
	}
	a.terminal = true
	a.stopLocked()
	return true
}

func (a *runningActivity) stopLocked() {
	if a.stopped {
		return
	}
	a.stopped = true
	close(a.done)
	a.cancel()
}

func (a *runningActivity) recordHeartbeat(record func() error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stopped {
		return errors.New("activity is no longer running")
	}
	return record()
}

func (w *Worker) complete(completion *corepb.ActivityTaskCompletion) error {
	completionBytes, err := proto.Marshal(completion)
	if err != nil {
		return fmt.Errorf("encode activity completion: %w", err)
	}
	return w.core.CompleteActivity(completionBytes)
}

func (w *Worker) recordHeartbeat(taskToken []byte, details []*commonpb.Payload) error {
	heartbeatMessage := &corepb.ActivityHeartbeat{TaskToken: taskToken, Details: details}
	if err := converter.EncodePayloads(heartbeatMessage, w.codec()); err != nil {
		return fmt.Errorf("encode activity heartbeat payloads: %w", err)
	}
	heartbeat, err := proto.Marshal(heartbeatMessage)
	if err != nil {
		return fmt.Errorf("encode activity heartbeat: %w", err)
	}
	return w.core.RecordActivityHeartbeat(heartbeat)
}

// RecordHeartbeat converts and records progress for the current activity through Temporal Core.
func RecordHeartbeat(ctx context.Context, details ...any) error {
	recorder, ok := ctx.Value(heartbeatContextKey{}).(heartbeatRecorder)
	if !ok {
		return errors.New("activity heartbeat called outside an activity handler")
	}
	payloadConverter, ok := ctx.Value(payloadConverterContextKey{}).(converter.PayloadConverter)
	if !ok {
		return errors.New("activity payload converter is missing from context")
	}
	payloads, err := payloadConverter.ToPayloads(details...)
	if err != nil {
		return fmt.Errorf("encode activity heartbeat: %w", err)
	}
	return recorder(payloads.GetPayloads())
}

func (w *Worker) enqueueActivityCompletion(
	running map[string]*runningActivity,
	activeExecutions *int,
	activity *runningActivity,
	completion *corepb.ActivityTaskCompletion,
	pipeline *completionpipe.Pipeline,
) error {
	if activity == nil || !activity.queueTerminal() {
		return nil
	}
	*activeExecutions--
	return w.enqueueCompletion(pipeline, completion, func() {
		key := string(activity.taskToken)
		if running[key] == activity {
			delete(running, key)
		}
	})
}

func (w *Worker) enqueueCompletion(
	pipeline *completionpipe.Pipeline,
	completion *corepb.ActivityTaskCompletion,
	finalize func(),
) error {
	if completion == nil {
		if finalize != nil {
			finalize()
		}
		return errors.New("activity execution produced no completion")
	}
	if err := converter.EncodePayloads(completion, w.codec()); err != nil {
		completion = failedCompletion(
			completion.TaskToken,
			fmt.Errorf("encode activity completion payloads: %w", err),
		)
	}
	return pipeline.Enqueue(completionpipe.Ticket{
		Weight: proto.Size(completion),
		Submit: func() error {
			return w.complete(completion)
		},
		Finalize: finalize,
	})
}

func (w *Worker) execute(ctx context.Context, taskToken []byte, start *activitytask.Start) *corepb.ActivityTaskCompletion {
	handler, ok := w.handlers[start.ActivityType]
	if !ok {
		return failedCompletion(taskToken, fmt.Errorf("activity %q is not registered", start.ActivityType))
	}
	result, err := handler.execute(ctx, w.payloadConverterOrDefault(), start.Input)
	if err != nil {
		return failedCompletion(taskToken, err)
	}
	return &corepb.ActivityTaskCompletion{
		TaskToken: taskToken,
		Result: &activityresult.ActivityExecutionResult{
			Status: &activityresult.ActivityExecutionResult_Completed{
				Completed: &activityresult.Success{Result: result},
			},
		},
	}
}

func (w *Worker) executeSafely(
	ctx context.Context,
	taskToken []byte,
	start *activitytask.Start,
) (completion *corepb.ActivityTaskCompletion) {
	defer func() {
		if value := recover(); value != nil {
			completion = panicCompletion(taskToken, start.ActivityType, value, debug.Stack())
		}
	}()
	return w.execute(ctx, taskToken, start)
}

func (w *Worker) payloadConverterOrDefault() converter.PayloadConverter {
	if w.payloadConverter != nil {
		return w.payloadConverter
	}
	return converter.GetDefaultPayloadConverter()
}

func (w *Worker) codec() converter.PayloadCodec {
	if w.payloadCodec != nil {
		return w.payloadCodec
	}
	return converter.NewNoopPayloadCodec()
}

func failedCompletion(taskToken []byte, err error) *corepb.ActivityTaskCompletion {
	return failedCompletionDetails(taskToken, err, "GoWasmActivityError", "")
}

func panicCompletion(taskToken []byte, activityType string, value any, stack []byte) *corepb.ActivityTaskCompletion {
	return failedCompletionDetails(
		taskToken,
		fmt.Errorf("activity %q panicked: %v", activityType, value),
		"PanicError",
		string(stack),
	)
}

func failedCompletionDetails(
	taskToken []byte,
	err error,
	failureType string,
	stackTrace string,
) *corepb.ActivityTaskCompletion {
	return &corepb.ActivityTaskCompletion{
		TaskToken: taskToken,
		Result: &activityresult.ActivityExecutionResult{
			Status: &activityresult.ActivityExecutionResult_Failed{
				Failed: &activityresult.Failure{
					Failure: &failurepb.Failure{
						Message:    err.Error(),
						Source:     "Go SDK on WASM Core prototype",
						StackTrace: stackTrace,
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
								Type: failureType,
							},
						},
					},
				},
			},
		},
	}
}

func cancelledCompletion(taskToken []byte, message string) *corepb.ActivityTaskCompletion {
	return &corepb.ActivityTaskCompletion{
		TaskToken: taskToken,
		Result: &activityresult.ActivityExecutionResult{
			Status: &activityresult.ActivityExecutionResult_Cancelled{
				Cancelled: &activityresult.Cancellation{
					Failure: &failurepb.Failure{
						Message: message,
						Source:  "Go SDK on WASM Core prototype",
						FailureInfo: &failurepb.Failure_CanceledFailureInfo{
							CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
						},
					},
				},
			},
		},
	}
}

func (w *Worker) activityCompletionLimit() int {
	if w.maxConcurrentActivityExecutions > 0 {
		return w.maxConcurrentActivityExecutions
	}
	return defaultMaxConcurrentActivityExecutions
}

func isActivityCancellationError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
