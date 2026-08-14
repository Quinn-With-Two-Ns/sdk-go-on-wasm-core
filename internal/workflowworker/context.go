package workflowworker

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	childworkflow "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/childworkflow"
	workflowcommands "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcommands"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ActivityOptions contains the small subset of remote-activity options supported by this worker.
type ActivityOptions struct {
	ActivityID            string
	ActivityType          string
	TaskQueue             string
	StartToCloseTimeout   time.Duration
	DisableEagerExecution bool
}

// LocalActivityOptions contains the timeouts supported for a local activity.
type LocalActivityOptions struct {
	ActivityID             string
	ActivityType           string
	ScheduleToCloseTimeout time.Duration
	ScheduleToStartTimeout time.Duration
	StartToCloseTimeout    time.Duration
	LocalRetryThreshold    time.Duration
}

// ChildWorkflowOptions contains the child-workflow options supported by this worker.
type ChildWorkflowOptions struct {
	Namespace                string
	WorkflowID               string
	WorkflowType             string
	TaskQueue                string
	WorkflowExecutionTimeout time.Duration
	WorkflowRunTimeout       time.Duration
	WorkflowTaskTimeout      time.Duration
	ParentClosePolicy        enumspb.ParentClosePolicy
	WorkflowIDReusePolicy    enumspb.WorkflowIdReusePolicy
}

// WorkflowExecution identifies a child workflow execution.
type WorkflowExecution struct {
	ID    string
	RunID string
}

// Future represents the result of an asynchronous workflow operation.
type Future interface {
	Get(ctx *Context, valuePtr any) error
	IsReady() bool
}

// Settable completes a Future created by NewFuture.
type Settable interface {
	Set(value any, err error)
	SetValue(value any)
	SetError(err error)
	Chain(Future)
}

// ChildWorkflowFuture exposes both child start and child completion readiness.
type ChildWorkflowFuture interface {
	Future
	GetChildWorkflowExecution() Future
}

// Context is the deterministic execution context for one workflow coroutine.
type Context struct {
	execution  *workflowExecution
	coroutine  *workflowCoroutine
	updateInfo *UpdateInfo
}

// Go starts a deterministic workflow coroutine. Only one workflow coroutine executes at a time.
func Go(ctx *Context, function func(*Context)) {
	GoNamed(ctx, "", function)
}

// GoNamed starts a named deterministic workflow coroutine.
func GoNamed(ctx *Context, name string, function func(*Context)) {
	if function == nil {
		panic("workflow Go requires a function")
	}
	ctx.requireRunning("Go")
	ctx.execution.addCoroutineWithUpdateInfo(name, ctx.updateInfo, function)
}

// NewFuture creates a manually completable workflow future.
func NewFuture(ctx *Context) (Future, Settable) {
	ctx.requireRunning("NewFuture")
	future := newFuture(ctx.execution)
	return future, future
}

// NewTimer emits a durable timer command and returns immediately.
func NewTimer(ctx *Context, duration time.Duration) Future {
	return ctx.newTimer(duration)
}

// ExecuteActivity schedules a remote activity and returns immediately.
func ExecuteActivity(ctx *Context, options ActivityOptions, arguments ...any) Future {
	return ctx.executeActivity(options, arguments...)
}

// ExecuteLocalActivity schedules an activity on the workflow worker without a server activity task.
func ExecuteLocalActivity(ctx *Context, options LocalActivityOptions, arguments ...any) Future {
	return ctx.executeLocalActivity(options, arguments...)
}

// ExecuteChildWorkflow starts a child workflow and returns immediately.
func ExecuteChildWorkflow(ctx *Context, options ChildWorkflowOptions, arguments ...any) ChildWorkflowFuture {
	return ctx.executeChildWorkflow(options, arguments...)
}

// Sleep waits on a durable timer.
func Sleep(ctx *Context, duration time.Duration) error {
	return ctx.sleep(duration)
}

func (c *Context) sleep(duration time.Duration) error {
	return c.newTimer(duration).Get(c, nil)
}

func (c *Context) newTimer(duration time.Duration) Future {
	c.requireRunning("NewTimer")
	if duration < 0 {
		return c.readyFuture(nil, errors.New("workflow timer duration cannot be negative"))
	}
	seq := c.execution.nextSeq()
	future := newFuture(c.execution)
	c.execution.operations[seq] = &workflowOperation{kind: operationTimer, result: future}
	c.execution.emit(&workflowcommands.WorkflowCommand{
		Variant: &workflowcommands.WorkflowCommand_StartTimer{
			StartTimer: &workflowcommands.StartTimer{
				Seq:                seq,
				StartToFireTimeout: durationpb.New(duration),
			},
		},
	})
	return future
}

func (c *Context) executeActivity(options ActivityOptions, arguments ...any) Future {
	c.requireRunning("ExecuteActivity")
	if options.ActivityType == "" {
		return c.readyFuture(nil, errors.New("activity type is required"))
	}
	if options.StartToCloseTimeout <= 0 {
		return c.readyFuture(nil, errors.New("activity start-to-close timeout must be positive"))
	}
	payloads, err := c.execution.payloadConverter.ToPayloads(arguments...)
	if err != nil {
		return c.readyFuture(nil, fmt.Errorf("encode activity input: %w", err))
	}

	seq := c.execution.nextSeq()
	activityID := options.ActivityID
	if activityID == "" {
		activityID = fmt.Sprintf("activity-%d", seq)
	}
	taskQueue := options.TaskQueue
	if taskQueue == "" {
		taskQueue = c.execution.taskQueue
	}
	future := newFuture(c.execution)
	c.execution.operations[seq] = &workflowOperation{kind: operationActivity, result: future}
	c.execution.emit(&workflowcommands.WorkflowCommand{
		Variant: &workflowcommands.WorkflowCommand_ScheduleActivity{
			ScheduleActivity: &workflowcommands.ScheduleActivity{
				Seq:                 seq,
				ActivityId:          activityID,
				ActivityType:        options.ActivityType,
				TaskQueue:           taskQueue,
				Arguments:           payloads.GetPayloads(),
				StartToCloseTimeout: durationpb.New(options.StartToCloseTimeout),
				DoNotEagerlyExecute: options.DisableEagerExecution,
			},
		},
	})
	return future
}

func (c *Context) executeLocalActivity(options LocalActivityOptions, arguments ...any) Future {
	c.requireRunning("ExecuteLocalActivity")
	if options.ActivityType == "" {
		return c.readyFuture(nil, errors.New("local activity type is required"))
	}
	if options.ScheduleToCloseTimeout <= 0 && options.StartToCloseTimeout <= 0 {
		return c.readyFuture(nil, errors.New("local activity schedule-to-close or start-to-close timeout must be positive"))
	}
	if options.ScheduleToCloseTimeout < 0 || options.ScheduleToStartTimeout < 0 ||
		options.StartToCloseTimeout < 0 || options.LocalRetryThreshold < 0 {
		return c.readyFuture(nil, errors.New("local activity timeouts cannot be negative"))
	}
	payloads, err := c.execution.payloadConverter.ToPayloads(arguments...)
	if err != nil {
		return c.readyFuture(nil, fmt.Errorf("encode local activity input: %w", err))
	}
	seq := c.execution.nextSeq()
	activityID := options.ActivityID
	if activityID == "" {
		activityID = fmt.Sprintf("local-activity-%d", seq)
	}
	future := newFuture(c.execution)
	c.execution.operations[seq] = &workflowOperation{kind: operationLocalActivity, result: future}
	command := &workflowcommands.ScheduleLocalActivity{
		Seq: seq, ActivityId: activityID, ActivityType: options.ActivityType,
		Arguments: payloads.GetPayloads(), Attempt: 1,
		CancellationType: workflowcommands.ActivityCancellationType_WAIT_CANCELLATION_COMPLETED,
	}
	if options.ScheduleToCloseTimeout > 0 {
		command.ScheduleToCloseTimeout = durationpb.New(options.ScheduleToCloseTimeout)
	}
	if options.ScheduleToStartTimeout > 0 {
		command.ScheduleToStartTimeout = durationpb.New(options.ScheduleToStartTimeout)
	}
	if options.StartToCloseTimeout > 0 {
		command.StartToCloseTimeout = durationpb.New(options.StartToCloseTimeout)
	}
	if options.LocalRetryThreshold > 0 {
		command.LocalRetryThreshold = durationpb.New(options.LocalRetryThreshold)
	}
	c.execution.emit(&workflowcommands.WorkflowCommand{Variant: &workflowcommands.WorkflowCommand_ScheduleLocalActivity{ScheduleLocalActivity: command}})
	return future
}

func (c *Context) executeChildWorkflow(options ChildWorkflowOptions, arguments ...any) ChildWorkflowFuture {
	c.requireRunning("ExecuteChildWorkflow")
	resultFuture := newFuture(c.execution)
	startedFuture := newFuture(c.execution)
	childFuture := &childWorkflowFuture{futureImpl: resultFuture, started: startedFuture}
	fail := func(err error) ChildWorkflowFuture {
		startedFuture.set(nil, err)
		resultFuture.set(nil, err)
		return childFuture
	}
	if options.WorkflowType == "" {
		return fail(errors.New("child workflow type is required"))
	}
	if options.WorkflowExecutionTimeout < 0 {
		return fail(errors.New("child workflow execution timeout cannot be negative"))
	}
	if options.WorkflowRunTimeout < 0 {
		return fail(errors.New("child workflow run timeout cannot be negative"))
	}
	if options.WorkflowTaskTimeout < 0 {
		return fail(errors.New("child workflow task timeout cannot be negative"))
	}
	payloads, err := c.execution.payloadConverter.ToPayloads(arguments...)
	if err != nil {
		return fail(fmt.Errorf("encode child workflow input: %w", err))
	}

	seq := c.execution.nextSeq()
	workflowID := options.WorkflowID
	if workflowID == "" {
		workflowID = fmt.Sprintf("%s_%d", c.execution.runID, seq)
	}
	taskQueue := options.TaskQueue
	if taskQueue == "" {
		taskQueue = c.execution.taskQueue
	}
	namespace := options.Namespace
	if namespace == "" {
		namespace = c.execution.namespace
	}
	workflowIDReusePolicy := options.WorkflowIDReusePolicy
	if workflowIDReusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED {
		workflowIDReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
	c.execution.operations[seq] = &workflowOperation{
		kind:       operationChild,
		result:     resultFuture,
		started:    startedFuture,
		workflowID: workflowID,
	}
	c.execution.emit(&workflowcommands.WorkflowCommand{
		Variant: &workflowcommands.WorkflowCommand_StartChildWorkflowExecution{
			StartChildWorkflowExecution: &workflowcommands.StartChildWorkflowExecution{
				Seq:                      seq,
				Namespace:                namespace,
				WorkflowId:               workflowID,
				WorkflowType:             options.WorkflowType,
				TaskQueue:                taskQueue,
				Input:                    payloads.GetPayloads(),
				WorkflowExecutionTimeout: optionalDuration(options.WorkflowExecutionTimeout),
				WorkflowRunTimeout:       optionalDuration(options.WorkflowRunTimeout),
				WorkflowTaskTimeout:      optionalDuration(options.WorkflowTaskTimeout),
				ParentClosePolicy:        childworkflow.ParentClosePolicy(options.ParentClosePolicy),
				WorkflowIdReusePolicy:    workflowIDReusePolicy,
			},
		},
	})
	return childFuture
}

func optionalDuration(duration time.Duration) *durationpb.Duration {
	if duration == 0 {
		return nil
	}
	return durationpb.New(duration)
}

func (c *Context) readyFuture(value any, err error) Future {
	future := newFuture(c.execution)
	future.set(value, err)
	return future
}

type futureImpl struct {
	execution *workflowExecution
	ready     bool
	value     any
	err       error
	chained   []*futureImpl
}

func newFuture(execution *workflowExecution) *futureImpl {
	return &futureImpl{execution: execution}
}

func (f *futureImpl) Get(ctx *Context, valuePtr any) error {
	if ctx == nil || ctx.execution != f.execution {
		return errors.New("future Get requires its workflow context")
	}
	if valuePtr != nil {
		value := reflect.ValueOf(valuePtr)
		if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
			return errors.New("future result must be a non-nil pointer")
		}
	}
	for !f.ready {
		if err := ctx.blockOn(f); err != nil {
			return err
		}
	}
	if f.err != nil || valuePtr == nil {
		return f.err
	}
	value := reflect.ValueOf(valuePtr)
	if f.value == nil {
		return errors.New("future completed without a result")
	}
	if payload, ok := f.value.(*commonpb.Payload); ok {
		if err := f.execution.payloadConverter.FromPayload(payload, valuePtr); err != nil {
			return fmt.Errorf("decode future result: %w", err)
		}
		return nil
	}
	result := reflect.ValueOf(f.value)
	if !result.IsValid() || !result.Type().AssignableTo(value.Elem().Type()) {
		return fmt.Errorf("future result type %T cannot be assigned to %T", f.value, valuePtr)
	}
	value.Elem().Set(result)
	return nil
}

func (f *futureImpl) IsReady() bool { return f.ready }

func (f *futureImpl) Set(value any, err error) {
	f.requireRunning("Set")
	f.set(value, err)
}

func (f *futureImpl) SetValue(value any) {
	f.requireRunning("SetValue")
	f.set(value, nil)
}

func (f *futureImpl) SetError(err error) {
	f.requireRunning("SetError")
	f.set(nil, err)
}

func (f *futureImpl) Chain(other Future) {
	f.requireRunning("Chain")
	otherFuture := unwrapFuture(other)
	if otherFuture == nil || otherFuture.execution != f.execution {
		panic("cannot chain a future from another workflow runtime")
	}
	if f.ready {
		panic("future is already set")
	}
	if otherFuture.ready {
		f.set(otherFuture.value, otherFuture.err)
		return
	}
	otherFuture.chained = append(otherFuture.chained, f)
}

func unwrapFuture(future Future) *futureImpl {
	switch future := future.(type) {
	case *futureImpl:
		return future
	case *childWorkflowFuture:
		return future.futureImpl
	default:
		return nil
	}
}

func (f *futureImpl) set(value any, err error) {
	if f.ready {
		panic("future is already set")
	}
	f.ready = true
	f.value = value
	f.err = err
	for _, chained := range f.chained {
		chained.set(value, err)
	}
	f.chained = nil
}

func (f *futureImpl) requireRunning(operation string) {
	if f.execution == nil || f.execution.dispatcher.current == nil {
		panic("workflow future " + operation + " must run in a workflow coroutine")
	}
}

type childWorkflowFuture struct {
	*futureImpl
	started *futureImpl
}

func (f *childWorkflowFuture) GetChildWorkflowExecution() Future { return f.started }

func (c *Context) blockOn(future *futureImpl) error {
	if c.coroutine == nil || c.execution.dispatcher.current != c.coroutine {
		return errors.New("future Get must be called from the currently running workflow coroutine")
	}
	c.coroutine.waiting = future
	if c.coroutine.yield == nil || !c.coroutine.yield(struct{}{}) {
		return errors.New("workflow execution was evicted")
	}
	return nil
}

func (c *Context) requireRunning(operation string) {
	if c == nil || c.execution == nil || c.coroutine == nil || c.execution.dispatcher.current != c.coroutine {
		panic("workflow " + operation + " must run in the currently executing workflow coroutine")
	}
}
