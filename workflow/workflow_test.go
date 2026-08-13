package workflow

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

type stubFuture struct {
	value      any
	err        error
	ready      bool
	decodeCall bool
	waitCall   bool
}

func (f *stubFuture) Get(_ *Context, valuePtr any) error {
	if valuePtr == nil {
		f.waitCall = true
		return f.err
	}
	f.decodeCall = true
	if f.err != nil {
		return f.err
	}
	reflect.ValueOf(valuePtr).Elem().Set(reflect.ValueOf(f.value))
	return nil
}

func (f *stubFuture) IsReady() bool { return f.ready }

type stubChildFuture struct {
	*stubFuture
	started UntypedFuture
}

func (f *stubChildFuture) GetChildWorkflowExecution() UntypedFuture { return f.started }

type stubSettable struct {
	value   any
	err     error
	chained UntypedFuture
}

func (s *stubSettable) Set(value any, err error) {
	s.value = value
	s.err = err
}

func (s *stubSettable) SetValue(value any) { s.value = value }
func (s *stubSettable) SetError(err error) { s.err = err }
func (s *stubSettable) Chain(future UntypedFuture) {
	s.chained = future
}

type namedOperations struct{}

func testActivity() {}

func (*namedOperations) activityMethod() {}

func TestFutureReturnsTypedResult(t *testing.T) {
	untyped := &stubFuture{value: "done", ready: true}
	future := Future[string]{future: untyped}

	result, err := future.Get(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != "done" || !untyped.decodeCall {
		t.Fatalf("Get() = %q, decode called = %t", result, untyped.decodeCall)
	}
	if !future.IsReady() || future.Untyped() != untyped {
		t.Fatal("typed future did not preserve readiness or its untyped form")
	}
}

func TestFutureWaitDoesNotDecodeResult(t *testing.T) {
	untyped := &stubFuture{value: "ignored"}
	if err := (Future[string]{future: untyped}).Wait(nil); err != nil {
		t.Fatal(err)
	}
	if !untyped.waitCall || untyped.decodeCall {
		t.Fatalf("wait called = %t, decode called = %t", untyped.waitCall, untyped.decodeCall)
	}
}

func TestFutureReturnsDecodeError(t *testing.T) {
	want := errors.New("decode failed")
	result, err := (Future[string]{future: &stubFuture{err: want}}).Get(nil)
	if !errors.Is(err, want) || result != "" {
		t.Fatalf("Get() = (%q, %v), want zero value and %v", result, err, want)
	}
}

func TestSettablePreservesTypedValuesAndFutureChain(t *testing.T) {
	untyped := &stubSettable{}
	settable := Settable[string]{settable: untyped}

	settable.SetValue("value")
	if untyped.value != "value" {
		t.Fatalf("SetValue() stored %#v, want value", untyped.value)
	}
	wantErr := errors.New("failed")
	settable.Set("partial", wantErr)
	if untyped.value != "partial" || !errors.Is(untyped.err, wantErr) {
		t.Fatalf("Set() stored (%#v, %v), want (partial, %v)", untyped.value, untyped.err, wantErr)
	}
	settable.SetError(wantErr)
	if !errors.Is(untyped.err, wantErr) {
		t.Fatalf("SetError() stored %v, want %v", untyped.err, wantErr)
	}
	source := &stubFuture{}
	settable.Chain(Future[string]{future: source})
	if untyped.chained != source || settable.Untyped() != untyped {
		t.Fatal("typed settable did not preserve its chained future or untyped form")
	}
}

func TestChildWorkflowFutureReturnsTypedStartFuture(t *testing.T) {
	want := WorkflowExecution{ID: "workflow-id", RunID: "run-id"}
	started := &stubFuture{value: want, ready: true}
	untyped := &stubChildFuture{stubFuture: &stubFuture{value: "child-result", ready: true}, started: started}
	future := ChildWorkflowFuture[string]{
		result: Future[string]{future: untyped},
		child:  untyped,
	}

	execution, err := future.GetChildWorkflowExecution().Get(nil)
	if err != nil {
		t.Fatal(err)
	}
	if execution != want || future.Untyped() != untyped {
		t.Fatalf("child start = %+v, want %+v", execution, want)
	}
	result, err := future.Get(nil)
	if err != nil || result != "child-result" || !future.IsReady() {
		t.Fatalf("child result = (%q, %v), ready = %t", result, err, future.IsReady())
	}
}

func TestOperationNameUsesFunctionAndMethodNames(t *testing.T) {
	if got := operationName("activity", testActivity); got != "testActivity" {
		t.Fatalf("function name = %q, want testActivity", got)
	}
	var operations *namedOperations
	if got := operationName("activity", operations.activityMethod); got != "activityMethod" {
		t.Fatalf("method name = %q, want activityMethod", got)
	}
}

func TestOperationNameRejectsUntypedTargets(t *testing.T) {
	for _, target := range []any{nil, "activity-name", 42} {
		t.Run(fmt.Sprintf("%T", target), func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered == nil || !strings.Contains(fmt.Sprint(recovered), "function") {
					t.Fatalf("panic = %v, want function-validation error", recovered)
				}
			}()
			operationName("activity", target)
		})
	}
}

func TestUntypedNamesAndOptionsMapToInternalCommands(t *testing.T) {
	activity := internalActivityOptions("dynamic-activity", ActivityOptions{
		ActivityID:            "activity-id",
		TaskQueue:             "activity-queue",
		StartToCloseTimeout:   time.Minute,
		DisableEagerExecution: true,
	})
	if activity.ActivityType != "dynamic-activity" || activity.ActivityID != "activity-id" ||
		activity.TaskQueue != "activity-queue" || activity.StartToCloseTimeout != time.Minute ||
		!activity.DisableEagerExecution {
		t.Fatalf("activity options were not preserved: %+v", activity)
	}

	child := internalChildWorkflowOptions("dynamic-workflow", ChildWorkflowOptions{
		Namespace:                "namespace",
		WorkflowID:               "workflow-id",
		TaskQueue:                "workflow-queue",
		WorkflowExecutionTimeout: time.Hour,
		WorkflowRunTimeout:       30 * time.Minute,
		WorkflowTaskTimeout:      10 * time.Second,
		ParentClosePolicy:        enumspb.PARENT_CLOSE_POLICY_ABANDON,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	})
	if child.WorkflowType != "dynamic-workflow" || child.Namespace != "namespace" ||
		child.WorkflowID != "workflow-id" || child.TaskQueue != "workflow-queue" ||
		child.WorkflowExecutionTimeout != time.Hour || child.WorkflowRunTimeout != 30*time.Minute ||
		child.WorkflowTaskTimeout != 10*time.Second ||
		child.ParentClosePolicy != enumspb.PARENT_CLOSE_POLICY_ABANDON ||
		child.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
		t.Fatalf("child workflow options were not preserved: %+v", child)
	}
}

func TestZeroFutureReturnsInitializationError(t *testing.T) {
	if _, err := (Future[string]{}).Get(nil); err == nil {
		t.Fatal("zero Future.Get returned nil error")
	}
	if err := (Future[string]{}).Wait(nil); err == nil {
		t.Fatal("zero Future.Wait returned nil error")
	}
	if (Future[string]{}).IsReady() {
		t.Fatal("zero Future reported ready")
	}
}

func TestContextHasNoExportedOperationMethods(t *testing.T) {
	contextType := reflect.TypeFor[*Context]()
	if contextType.NumMethod() != 0 {
		methods := make([]string, 0, contextType.NumMethod())
		for i := range contextType.NumMethod() {
			methods = append(methods, contextType.Method(i).Name)
		}
		t.Fatalf("workflow.Context exports methods %v; workflow operations must be package functions", methods)
	}
}
