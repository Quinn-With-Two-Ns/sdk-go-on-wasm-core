// Package workflow contains APIs used by workflow implementations.
//
// Workflow code must remain deterministic. Blocking operations such as timers,
// activities, and child workflows are represented by Futures and are resumed by
// the worker as matching history events arrive.
package workflow

import (
	"fmt"
	"time"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/functionname"
	internalworkflow "github.com/temporalio/sdk-go-on-wasm-core/internal/workflowworker"
	enumspb "go.temporal.io/api/enums/v1"
)

type (
	// Context is the deterministic execution context for one workflow coroutine.
	Context = internalworkflow.Context
	// WorkflowExecution identifies a child workflow execution.
	WorkflowExecution = internalworkflow.WorkflowExecution
	// UntypedFuture represents an asynchronous result decoded through a caller-provided pointer.
	UntypedFuture = internalworkflow.Future
	// UntypedSettable completes an UntypedFuture created by NewFutureUntyped.
	UntypedSettable = internalworkflow.Settable
	// UntypedChildWorkflowFuture exposes untyped child start and completion readiness.
	UntypedChildWorkflowFuture = internalworkflow.ChildWorkflowFuture
)

// ActivityOptions contains the supported remote-activity options.
// The activity type comes from ExecuteActivity's function or ExecuteActivityUntyped's name.
type ActivityOptions struct {
	ActivityID          string
	TaskQueue           string
	StartToCloseTimeout time.Duration
	// DisableEagerExecution prevents same-worker eager dispatch for this activity.
	// Eager execution is enabled by default when a combined worker hosts both task types
	// on the same task queue.
	DisableEagerExecution bool
}

// ChildWorkflowOptions contains the supported child-workflow options.
// The workflow type comes from ExecuteChildWorkflow's function or ExecuteChildWorkflowUntyped's name.
type ChildWorkflowOptions struct {
	Namespace                string
	WorkflowID               string
	TaskQueue                string
	WorkflowExecutionTimeout time.Duration
	WorkflowRunTimeout       time.Duration
	WorkflowTaskTimeout      time.Duration
	ParentClosePolicy        enumspb.ParentClosePolicy
	WorkflowIDReusePolicy    enumspb.WorkflowIdReusePolicy
}

// Future represents an asynchronous result of type T.
//
// Use Get to wait for and decode the result, or Wait when only completion is needed.
// Untyped exposes the pointer-based future for dynamic plumbing.
type Future[T any] struct {
	future UntypedFuture
}

// Get waits for the future and returns its typed result.
func (f Future[T]) Get(ctx *Context) (T, error) {
	var result T
	if f.future == nil {
		return result, fmt.Errorf("workflow future is not initialized")
	}
	err := f.future.Get(ctx, &result)
	return result, err
}

// Wait waits for completion without decoding a result.
func (f Future[T]) Wait(ctx *Context) error {
	if f.future == nil {
		return fmt.Errorf("workflow future is not initialized")
	}
	return f.future.Get(ctx, nil)
}

// IsReady reports whether Get or Wait can return without blocking.
func (f Future[T]) IsReady() bool {
	return f.future != nil && f.future.IsReady()
}

// Untyped returns the pointer-decoded form of the future.
func (f Future[T]) Untyped() UntypedFuture {
	return f.future
}

// Settable completes a Future of type T created by NewFuture.
type Settable[T any] struct {
	settable UntypedSettable
}

// Set completes the future with either value or err.
func (s Settable[T]) Set(value T, err error) {
	s.settable.Set(value, err)
}

// SetValue completes the future successfully with value.
func (s Settable[T]) SetValue(value T) {
	s.settable.SetValue(value)
}

// SetError completes the future with err.
func (s Settable[T]) SetError(err error) {
	s.settable.SetError(err)
}

// Chain completes the future when source completes.
func (s Settable[T]) Chain(source Future[T]) {
	s.settable.Chain(source.future)
}

// Untyped returns the dynamically typed form of the settable.
func (s Settable[T]) Untyped() UntypedSettable {
	return s.settable
}

// ChildWorkflowFuture represents a child workflow result of type T.
type ChildWorkflowFuture[T any] struct {
	result Future[T]
	child  UntypedChildWorkflowFuture
}

// Get waits for the child workflow and returns its typed result.
func (f ChildWorkflowFuture[T]) Get(ctx *Context) (T, error) {
	return f.result.Get(ctx)
}

// Wait waits for the child workflow without decoding its result.
func (f ChildWorkflowFuture[T]) Wait(ctx *Context) error {
	return f.result.Wait(ctx)
}

// IsReady reports whether Get or Wait can return without blocking.
func (f ChildWorkflowFuture[T]) IsReady() bool {
	return f.result.IsReady()
}

// GetChildWorkflowExecution returns a typed future that becomes ready when the child starts.
func (f ChildWorkflowFuture[T]) GetChildWorkflowExecution() Future[WorkflowExecution] {
	if f.child == nil {
		return Future[WorkflowExecution]{}
	}
	return Future[WorkflowExecution]{future: f.child.GetChildWorkflowExecution()}
}

// Untyped returns the pointer-decoded form of the child workflow future.
func (f ChildWorkflowFuture[T]) Untyped() UntypedChildWorkflowFuture {
	return f.child
}

// Go starts a deterministic workflow coroutine. Only one workflow coroutine executes at a time.
func Go(ctx *Context, function func(*Context)) {
	internalworkflow.Go(ctx, function)
}

// GoNamed starts a named deterministic workflow coroutine.
func GoNamed(ctx *Context, name string, function func(*Context)) {
	internalworkflow.GoNamed(ctx, name, function)
}

// NewFuture creates a manually completable workflow future of type T.
func NewFuture[T any](ctx *Context) (Future[T], Settable[T]) {
	future, settable := internalworkflow.NewFuture(ctx)
	return Future[T]{future: future}, Settable[T]{settable: settable}
}

// NewFutureUntyped creates a manually completable future for dynamic plumbing.
//
// Prefer NewFuture when the result type is known. The result of this function
// is decoded by passing a pointer to UntypedFuture.Get.
func NewFutureUntyped(ctx *Context) (UntypedFuture, UntypedSettable) {
	return internalworkflow.NewFuture(ctx)
}

// Sleep waits on a durable timer.
func Sleep(ctx *Context, duration time.Duration) error {
	return internalworkflow.Sleep(ctx, duration)
}

// NewTimer emits a durable timer command and returns immediately.
func NewTimer(ctx *Context, duration time.Duration) UntypedFuture {
	return internalworkflow.NewTimer(ctx, duration)
}

// ExecuteActivity schedules activity and returns a future for its result of type T.
//
// activity must be the registered activity function. Its implementation is not
// called; its function name identifies the activity type in workflow history.
func ExecuteActivity[T any](
	ctx *Context,
	activity any,
	options ActivityOptions,
	arguments ...any,
) Future[T] {
	return Future[T]{future: internalworkflow.ExecuteActivity(
		ctx,
		internalActivityOptions(operationName("activity", activity), options),
		arguments...,
	)}
}

// ExecuteActivityUntyped schedules an activity identified by name.
//
// Use this dynamic escape hatch when a function reference is unavailable. The
// result is decoded by passing a pointer to UntypedFuture.Get.
func ExecuteActivityUntyped(
	ctx *Context,
	activityName string,
	options ActivityOptions,
	arguments ...any,
) UntypedFuture {
	return internalworkflow.ExecuteActivity(ctx, internalActivityOptions(activityName, options), arguments...)
}

// ExecuteChildWorkflow starts childWorkflow and returns a future for its result of type T.
//
// childWorkflow must be the registered workflow function. Its implementation is
// not called; its function name identifies the workflow type in workflow history.
func ExecuteChildWorkflow[T any](
	ctx *Context,
	childWorkflow any,
	options ChildWorkflowOptions,
	arguments ...any,
) ChildWorkflowFuture[T] {
	child := internalworkflow.ExecuteChildWorkflow(
		ctx,
		internalChildWorkflowOptions(operationName("workflow", childWorkflow), options),
		arguments...,
	)
	return ChildWorkflowFuture[T]{result: Future[T]{future: child}, child: child}
}

// ExecuteChildWorkflowUntyped starts a child workflow identified by name.
//
// Use this dynamic escape hatch when a function reference is unavailable. The
// result is decoded by passing a pointer to UntypedFuture.Get.
func ExecuteChildWorkflowUntyped(
	ctx *Context,
	workflowName string,
	options ChildWorkflowOptions,
	arguments ...any,
) UntypedChildWorkflowFuture {
	return internalworkflow.ExecuteChildWorkflow(
		ctx,
		internalChildWorkflowOptions(workflowName, options),
		arguments...,
	)
}

func internalActivityOptions(name string, options ActivityOptions) internalworkflow.ActivityOptions {
	return internalworkflow.ActivityOptions{
		ActivityID:            options.ActivityID,
		ActivityType:          name,
		TaskQueue:             options.TaskQueue,
		StartToCloseTimeout:   options.StartToCloseTimeout,
		DisableEagerExecution: options.DisableEagerExecution,
	}
}

func internalChildWorkflowOptions(name string, options ChildWorkflowOptions) internalworkflow.ChildWorkflowOptions {
	return internalworkflow.ChildWorkflowOptions{
		Namespace:                options.Namespace,
		WorkflowID:               options.WorkflowID,
		WorkflowType:             name,
		TaskQueue:                options.TaskQueue,
		WorkflowExecutionTimeout: options.WorkflowExecutionTimeout,
		WorkflowRunTimeout:       options.WorkflowRunTimeout,
		WorkflowTaskTimeout:      options.WorkflowTaskTimeout,
		ParentClosePolicy:        options.ParentClosePolicy,
		WorkflowIDReusePolicy:    options.WorkflowIDReusePolicy,
	}
}

func operationName(kind string, operation any) string {
	name, err := functionname.Get(kind, operation)
	if err != nil {
		panic("workflow execute " + err.Error())
	}
	return name
}
