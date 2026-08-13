package activityworker

import (
	"context"
	"crypto/tls"
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
	corepb "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb"
	activityresult "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/activityresult"
	activitytask "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/activitytask"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

func TestNewForwardsConnectionOptions(t *testing.T) {
	oldNewActivityCoreWorker := newActivityCoreWorker
	defer func() { newActivityCoreWorker = oldNewActivityCoreWorker }()

	tlsConfig := &tls.Config{ServerName: "example.tmprl.cloud"}
	var gotAddress, gotNamespace, gotTaskQueue, gotIdentity, gotAPIKey string
	var gotTLS *tls.Config
	newActivityCoreWorker = func(
		address, namespace, taskQueue, identity string,
		_, _ int32,
		forwardedTLS *tls.Config,
		apiKey string,
	) (coreWorker, error) {
		gotAddress = address
		gotNamespace = namespace
		gotTaskQueue = taskQueue
		gotIdentity = identity
		gotTLS = forwardedTLS
		gotAPIKey = apiKey
		return newFakeCore(), nil
	}

	_, err := New(Options{
		Address:   "example.tmprl.cloud:7233",
		Namespace: "example.account",
		TaskQueue: "queue",
		Identity:  "worker",
		TLS:       tlsConfig,
		APIKey:    "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotAddress != "example.tmprl.cloud:7233" || gotNamespace != "example.account" ||
		gotTaskQueue != "queue" || gotIdentity != "worker" {
		t.Fatalf("New forwarded unexpected worker identity: %q %q %q %q", gotAddress, gotNamespace, gotTaskQueue, gotIdentity)
	}
	if gotTLS != tlsConfig || gotAPIKey != "secret" {
		t.Fatalf("New forwarded TLS/API key = %p/%q, want %p/%q", gotTLS, gotAPIKey, tlsConfig, "secret")
	}
}

func TestNewNormalizesSharedOptions(t *testing.T) {
	oldNewActivityCoreWorker := newActivityCoreWorker
	defer func() { newActivityCoreWorker = oldNewActivityCoreWorker }()

	var gotAddress, gotNamespace, gotTaskQueue, gotIdentity string
	var gotExecutions, gotPollers int32
	newActivityCoreWorker = func(
		address, namespace, taskQueue, identity string,
		executions, pollers int32,
		_ *tls.Config,
		_ string,
	) (coreWorker, error) {
		gotAddress = address
		gotNamespace = namespace
		gotTaskQueue = taskQueue
		gotIdentity = identity
		gotExecutions = executions
		gotPollers = pollers
		return newFakeCore(), nil
	}

	worker, err := New(Options{TaskQueue: "queue"})
	if err != nil {
		t.Fatal(err)
	}
	if gotAddress != "localhost:7233" || gotNamespace != "default" || gotTaskQueue != "queue" || gotIdentity != "go-sdk-on-wasm-core" {
		t.Fatalf("New forwarded unexpected defaults: %q %q %q %q", gotAddress, gotNamespace, gotTaskQueue, gotIdentity)
	}
	if gotExecutions != defaultMaxConcurrentActivityExecutions || gotPollers != 0 {
		t.Fatalf("New forwarded limits = %d/%d, want %d/0", gotExecutions, gotPollers, defaultMaxConcurrentActivityExecutions)
	}
	if worker.payloadConverter == nil {
		t.Fatal("New did not install the default payload converter")
	}
	if worker.payloadCodec == nil {
		t.Fatal("New did not install the default payload codec")
	}
}

func TestNewWithCoreUsesProvidedCore(t *testing.T) {
	oldNewActivityCoreWorker := newActivityCoreWorker
	defer func() { newActivityCoreWorker = oldNewActivityCoreWorker }()
	newActivityCoreWorker = func(string, string, string, string, int32, int32, *tls.Config, string) (coreWorker, error) {
		t.Fatal("NewWithCore reached core construction")
		return nil, nil
	}

	core := newFakeCore()
	worker, err := NewWithCore(Options{TaskQueue: "queue"}, core)
	if err != nil {
		t.Fatal(err)
	}
	if worker.core != core {
		t.Fatal("NewWithCore did not reuse the provided core worker")
	}
	if worker.maxConcurrentActivityExecutions != defaultMaxConcurrentActivityExecutions {
		t.Fatalf("worker concurrency limit = %d, want %d", worker.maxConcurrentActivityExecutions, defaultMaxConcurrentActivityExecutions)
	}
}

func TestNewRejectsInvalidActivityOptions(t *testing.T) {
	oldNewActivityCoreWorker := newActivityCoreWorker
	defer func() { newActivityCoreWorker = oldNewActivityCoreWorker }()
	newActivityCoreWorker = func(string, string, string, string, int32, int32, *tls.Config, string) (coreWorker, error) {
		t.Fatal("New reached core construction for invalid options")
		return nil, nil
	}

	testCases := []struct {
		name    string
		options Options
		want    string
	}{
		{name: "missing task queue", options: Options{}, want: "activity task queue is required"},
		{name: "negative executions", options: Options{TaskQueue: "queue", MaxConcurrentActivityExecutionSize: -1}, want: "maximum concurrent activity executions must be between 1 and 2147483647"},
		{name: "execution overflow", options: Options{TaskQueue: "queue", MaxConcurrentActivityExecutionSize: math.MaxInt32 + 1}, want: "maximum concurrent activity executions must be between 1 and 2147483647"},
		{name: "negative pollers", options: Options{TaskQueue: "queue", MaxConcurrentActivityTaskPollers: -1}, want: "maximum concurrent activity task pollers must be between 0 and 2147483647"},
		{name: "poller overflow", options: Options{TaskQueue: "queue", MaxConcurrentActivityTaskPollers: math.MaxInt32 + 1}, want: "maximum concurrent activity task pollers must be between 0 and 2147483647"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := New(testCase.options)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("New error = %v, want substring %q", err, testCase.want)
			}
		})
	}
}

func TestExecuteConvertsTypedInputAndResult(t *testing.T) {
	w := testWorker(t, "greet", func(ctx context.Context, greeting string, name string) (string, error) {
		if ctx == nil {
			t.Fatal("activity context is nil")
		}
		return greeting + ", " + name, nil
	})
	input, err := w.payloadConverterOrDefault().ToPayloads("Hello", "Temporal")
	if err != nil {
		t.Fatal(err)
	}

	completion := w.execute(context.Background(), []byte("token"), &activitytask.Start{
		ActivityType: "greet",
		Input:        input.Payloads,
	})
	completed, ok := completion.Result.Status.(*activityresult.ActivityExecutionResult_Completed)
	if !ok {
		t.Fatalf("completion status is %T, want completed", completion.Result.Status)
	}
	if got := string(completed.Completed.Result.Metadata[converter.MetadataEncoding]); got != converter.MetadataEncodingJSON {
		t.Fatalf("completion encoding is %q, want %q", got, converter.MetadataEncodingJSON)
	}
	var result string
	if err := w.payloadConverterOrDefault().FromPayload(completed.Completed.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result != "Hello, Temporal" {
		t.Fatalf("completion result is %q", result)
	}
}

func TestExecuteBuildsCoreFailure(t *testing.T) {
	w := testWorker(t, "fail", func() error {
		return errors.New("expected failure")
	})
	completion := w.execute(context.Background(), []byte("token"), &activitytask.Start{ActivityType: "fail"})
	failed, ok := completion.Result.Status.(*activityresult.ActivityExecutionResult_Failed)
	if !ok {
		t.Fatalf("completion status is %T, want failed", completion.Result.Status)
	}
	if got := failed.Failed.Failure.Message; got != "expected failure" {
		t.Fatalf("failure message is %q", got)
	}
}

func TestExecuteReportsDataConversionFailure(t *testing.T) {
	w := testWorker(t, "takes-string", func(string) error { return nil })
	completion := w.execute(context.Background(), []byte("token"), &activitytask.Start{
		ActivityType: "takes-string",
		Input: []*commonpb.Payload{{
			Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON)},
			Data:     []byte("not JSON"),
		}},
	})
	failed, ok := completion.Result.Status.(*activityresult.ActivityExecutionResult_Failed)
	if !ok {
		t.Fatalf("completion status is %T, want failed", completion.Result.Status)
	}
	if got := failed.Failed.Failure.Message; got == "" {
		t.Fatal("conversion failure has no message")
	}
}

func TestRegisterRejectsInvalidSignature(t *testing.T) {
	w := &Worker{handlers: make(map[string]*registeredActivity)}
	defer func() {
		if recover() == nil {
			t.Fatal("Register accepted a function without an error result")
		}
	}()
	w.Register("invalid", func() string { return "invalid" })
}

func TestCancelledCompletionUsesCoreCancellationResult(t *testing.T) {
	completion := cancelledCompletion([]byte("token"), "CANCELLED")
	cancelled, ok := completion.Result.Status.(*activityresult.ActivityExecutionResult_Cancelled)
	if !ok {
		t.Fatalf("completion status is %T, want cancelled", completion.Result.Status)
	}
	if got := cancelled.Cancelled.Failure.Message; got != "CANCELLED" {
		t.Fatalf("cancellation message is %q", got)
	}
	if cancelled.Cancelled.Failure.GetCanceledFailureInfo() == nil {
		t.Fatal("cancellation failure is missing CanceledFailureInfo")
	}
}

func TestRecordHeartbeatConvertsDetails(t *testing.T) {
	payloadConverter := converter.GetDefaultPayloadConverter()
	var details []*commonpb.Payload
	ctx := context.WithValue(context.Background(), heartbeatContextKey{}, heartbeatRecorder(func(got []*commonpb.Payload) error {
		details = got
		return nil
	}))
	ctx = context.WithValue(ctx, payloadConverterContextKey{}, payloadConverter)

	if err := RecordHeartbeat(ctx, "working", 42); err != nil {
		t.Fatal(err)
	}
	var status string
	var count int
	if err := payloadConverter.FromPayloads(&commonpb.Payloads{Payloads: details}, &status, &count); err != nil {
		t.Fatal(err)
	}
	if status != "working" || count != 42 {
		t.Fatalf("heartbeat details are %q, %d", status, count)
	}
}

func TestRecordHeartbeatRejectsNonActivityContext(t *testing.T) {
	if err := RecordHeartbeat(context.Background()); err == nil {
		t.Fatal("RecordHeartbeat succeeded outside an activity handler")
	}
}

func TestRunExecutesActivitiesInParallel(t *testing.T) {
	core := newFakeCore()
	w := runTestWorker(t, core, 2)
	var startedCount atomic.Int32
	bothStarted := make(chan struct{})
	release := make(chan struct{})
	w.Register("parallel", func() error {
		if startedCount.Add(1) == 2 {
			close(bothStarted)
		}
		<-release
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- w.Run(ctx) }()
	core.sendStart(t, "first", "parallel")
	core.sendStart(t, "second", "parallel")
	waitForSignal(t, bothStarted, "both activities to start")
	close(release)
	for range 2 {
		completion := core.nextCompletion(t)
		if _, ok := completion.Result.Status.(*activityresult.ActivityExecutionResult_Completed); !ok {
			t.Fatalf("completion status is %T, want completed", completion.Result.Status)
		}
	}
	cancel()
	waitForRun(t, runResult)
}

func TestRunSeparatesPayloadConversionFromCoreBoundaryCodec(t *testing.T) {
	core := newFakeCore()
	w := runTestWorker(t, core, 1)
	codec := activityBoundaryCodec{}
	w.payloadCodec = codec
	w.Register("greet", func(ctx context.Context, name string) (string, error) {
		if err := RecordHeartbeat(ctx, "working", name); err != nil {
			return "", err
		}
		return "Hello, " + name, nil
	})

	input, err := w.payloadConverter.ToPayload("Temporal")
	if err != nil {
		t.Fatal(err)
	}
	encodedInput, err := codec.Encode([]*commonpb.Payload{input})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- w.Run(ctx) }()
	core.tasks <- &activitytask.ActivityTask{
		TaskToken: []byte("codec-token"),
		Variant: &activitytask.ActivityTask_Start{Start: &activitytask.Start{
			ActivityType: "greet",
			Input:        encodedInput,
		}},
	}

	heartbeat := core.nextHeartbeat(t)
	for _, detail := range heartbeat.Details {
		if string(detail.Metadata["codec"]) != "boundary" {
			t.Fatal("heartbeat detail was not encoded at the Core boundary")
		}
	}
	completion := core.nextCompletion(t)
	result := completion.Result.GetCompleted().Result
	if string(result.Metadata["codec"]) != "boundary" {
		t.Fatal("activity result was not encoded at the Core boundary")
	}
	decodedResult, err := codec.Decode([]*commonpb.Payload{result})
	if err != nil {
		t.Fatal(err)
	}
	var greeting string
	if err := w.payloadConverter.FromPayload(decodedResult[0], &greeting); err != nil {
		t.Fatal(err)
	}
	if greeting != "Hello, Temporal" {
		t.Fatalf("activity result = %q", greeting)
	}

	cancel()
	waitForRun(t, runResult)
}

func TestRunTurnsPayloadCodecErrorsIntoActivityFailures(t *testing.T) {
	tests := []struct {
		name   string
		codec  converter.PayloadCodec
		prefix string
	}{
		{
			name:   "decode",
			codec:  failingActivityCodec{decodeErr: errors.New("decode unavailable")},
			prefix: "decode activity task payloads:",
		},
		{
			name:   "encode",
			codec:  failingActivityCodec{encodeErr: errors.New("encode unavailable")},
			prefix: "encode activity completion payloads:",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core := newFakeCore()
			w := runTestWorker(t, core, 1)
			w.payloadCodec = test.codec
			w.Register("result", func(string) (string, error) { return "done", nil })

			input, err := w.payloadConverter.ToPayload("input")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			runResult := make(chan error, 1)
			go func() { runResult <- w.Run(ctx) }()
			core.tasks <- &activitytask.ActivityTask{
				TaskToken: []byte("codec-error-token"),
				Variant: &activitytask.ActivityTask_Start{Start: &activitytask.Start{
					ActivityType: "result",
					Input:        []*commonpb.Payload{input},
				}},
			}

			completion := core.nextCompletion(t)
			failed := completion.Result.GetFailed()
			if failed == nil || failed.Failure == nil ||
				!strings.Contains(failed.Failure.Message, test.prefix) ||
				!strings.Contains(failed.Failure.Message, test.name+" unavailable") {
				t.Fatalf("codec failure completion = %v, want %s context", completion, test.name)
			}

			cancel()
			waitForRun(t, runResult)
		})
	}
}

func TestRunCompletesCancellationWithoutWaitingForHandler(t *testing.T) {
	core := newFakeCore()
	w := runTestWorker(t, core, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	w.Register("ignores-cancellation", func(context.Context) error {
		close(started)
		<-release
		return nil
	})

	ctx, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- w.Run(ctx) }()
	core.sendStart(t, "token", "ignores-cancellation")
	waitForSignal(t, started, "activity to start")
	core.sendCancel(t, "token", activitytask.ActivityCancelReason_CANCELLED)
	completion := core.nextCompletion(t)
	if _, ok := completion.Result.Status.(*activityresult.ActivityExecutionResult_Cancelled); !ok {
		t.Fatalf("completion status is %T, want cancelled", completion.Result.Status)
	}
	cancelRun()
	waitForRun(t, runResult)
	close(release)
}

func TestRunShutdownDoesNotWaitForHandler(t *testing.T) {
	core := newFakeCore()
	w := runTestWorker(t, core, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	w.Register("ignores-shutdown", func(context.Context) error {
		close(started)
		<-release
		return nil
	})

	ctx, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- w.Run(ctx) }()
	core.sendStart(t, "token", "ignores-shutdown")
	waitForSignal(t, started, "activity to start")
	cancelRun()
	completion := core.nextCompletion(t)
	if _, ok := completion.Result.Status.(*activityresult.ActivityExecutionResult_Cancelled); !ok {
		t.Fatalf("completion status is %T, want cancelled", completion.Result.Status)
	}
	waitForRun(t, runResult)
	close(release)
}

func TestExecuteSafelyConvertsPanicToFailure(t *testing.T) {
	w := testWorker(t, "panic", func() error { panic("boom") })
	completion := w.executeSafely(
		context.Background(),
		[]byte("token"),
		&activitytask.Start{ActivityType: "panic"},
	)
	failed, ok := completion.Result.Status.(*activityresult.ActivityExecutionResult_Failed)
	if !ok {
		t.Fatalf("completion status is %T, want failed", completion.Result.Status)
	}
	if !strings.Contains(failed.Failed.Failure.Message, "panicked: boom") {
		t.Fatalf("panic failure message is %q", failed.Failed.Failure.Message)
	}
	if failed.Failed.Failure.StackTrace == "" {
		t.Fatal("panic failure has no stack trace")
	}
	if got := failed.Failed.Failure.GetApplicationFailureInfo().Type; got != "PanicError" {
		t.Fatalf("panic failure type is %q, want PanicError", got)
	}
}

func testWorker(t *testing.T, name string, function any) *Worker {
	t.Helper()
	activity, err := newRegisteredActivity(name, function)
	if err != nil {
		t.Fatal(err)
	}
	return &Worker{
		payloadConverter: converter.GetDefaultPayloadConverter(),
		handlers:         map[string]*registeredActivity{name: activity},
	}
}

type fakeCore struct {
	tasks       chan *activitytask.ActivityTask
	attempts    chan *corepb.ActivityTaskCompletion
	completions chan *corepb.ActivityTaskCompletion
	heartbeats  chan *corepb.ActivityHeartbeat
	onComplete  func(*corepb.ActivityTaskCompletion) error
}

func newFakeCore() *fakeCore {
	return &fakeCore{
		tasks:       make(chan *activitytask.ActivityTask, 8),
		attempts:    make(chan *corepb.ActivityTaskCompletion, 8),
		completions: make(chan *corepb.ActivityTaskCompletion, 8),
		heartbeats:  make(chan *corepb.ActivityHeartbeat, 8),
	}
}

func (f *fakeCore) PollActivity(ctx context.Context) ([]byte, error) {
	select {
	case task := <-f.tasks:
		return proto.Marshal(task)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeCore) CompleteActivity(encoded []byte) error {
	completion := &corepb.ActivityTaskCompletion{}
	if err := proto.Unmarshal(encoded, completion); err != nil {
		return err
	}
	f.attempts <- completion
	if f.onComplete != nil {
		if err := f.onComplete(completion); err != nil {
			return err
		}
	}
	f.completions <- completion
	return nil
}

func (f *fakeCore) RecordActivityHeartbeat(encoded []byte) error {
	heartbeat := &corepb.ActivityHeartbeat{}
	if err := proto.Unmarshal(encoded, heartbeat); err != nil {
		return err
	}
	f.heartbeats <- heartbeat
	return nil
}
func (*fakeCore) Shutdown() error { return nil }

func (f *fakeCore) sendStart(t *testing.T, token, activityType string) {
	t.Helper()
	f.tasks <- &activitytask.ActivityTask{
		TaskToken: []byte(token),
		Variant: &activitytask.ActivityTask_Start{
			Start: &activitytask.Start{ActivityType: activityType},
		},
	}
}

func (f *fakeCore) sendCancel(t *testing.T, token string, reason activitytask.ActivityCancelReason) {
	t.Helper()
	f.tasks <- &activitytask.ActivityTask{
		TaskToken: []byte(token),
		Variant: &activitytask.ActivityTask_Cancel{
			Cancel: &activitytask.Cancel{Reason: reason},
		},
	}
}

func (f *fakeCore) nextCompletion(t *testing.T) *corepb.ActivityTaskCompletion {
	t.Helper()
	select {
	case completion := <-f.completions:
		return completion
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for activity completion")
		return nil
	}
}

func (f *fakeCore) nextAttempt(t *testing.T) *corepb.ActivityTaskCompletion {
	t.Helper()
	select {
	case completion := <-f.attempts:
		return completion
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for activity completion attempt")
		return nil
	}
}

func (f *fakeCore) nextHeartbeat(t *testing.T) *corepb.ActivityHeartbeat {
	t.Helper()
	select {
	case heartbeat := <-f.heartbeats:
		return heartbeat
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for activity heartbeat")
		return nil
	}
}

func runTestWorker(t *testing.T, core *fakeCore, maxConcurrent int) *Worker {
	t.Helper()
	return &Worker{
		core:                            core,
		payloadConverter:                converter.GetDefaultPayloadConverter(),
		handlers:                        make(map[string]*registeredActivity),
		maxConcurrentActivityExecutions: maxConcurrent,
	}
}

type activityBoundaryCodec struct{}

func (activityBoundaryCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for i, payload := range payloads {
		result[i] = proto.Clone(payload).(*commonpb.Payload)
		if result[i].Metadata == nil {
			result[i].Metadata = make(map[string][]byte)
		}
		result[i].Metadata["codec"] = []byte("boundary")
		result[i].Data = append([]byte("coded:"), result[i].Data...)
	}
	return result, nil
}

type failingActivityCodec struct {
	encodeErr error
	decodeErr error
}

func (c failingActivityCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return payloads, c.encodeErr
}

func (c failingActivityCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return payloads, c.decodeErr
}

func (activityBoundaryCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for i, payload := range payloads {
		if string(payload.Metadata["codec"]) != "boundary" || !strings.HasPrefix(string(payload.Data), "coded:") {
			return nil, errors.New("payload is not boundary encoded")
		}
		result[i] = proto.Clone(payload).(*commonpb.Payload)
		delete(result[i].Metadata, "codec")
		result[i].Data = result[i].Data[len("coded:"):]
	}
	return result, nil
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitForRun(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func waitForRunError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("worker returned nil error")
		}
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop")
		return nil
	}
}

func TestRunSubmitsActivityCompletionsConcurrently(t *testing.T) {
	core := newFakeCore()
	started := make(chan string, 2)
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	core.onComplete = func(completion *corepb.ActivityTaskCompletion) error {
		current := active.Add(1)
		for {
			previous := maxActive.Load()
			if current <= previous || maxActive.CompareAndSwap(previous, current) {
				break
			}
		}
		started <- string(completion.TaskToken)
		defer active.Add(-1)
		if string(completion.TaskToken) == "first" {
			<-releaseFirst
			return nil
		}
		<-releaseSecond
		return nil
	}

	w := runTestWorker(t, core, 2)
	w.Register("complete", func() error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- w.Run(ctx) }()

	core.sendStart(t, "first", "complete")
	core.sendStart(t, "second", "complete")
	startedTokens := []string{
		waitForString(t, started, "first completion to start"),
		waitForString(t, started, "second completion to start"),
	}
	if !sameStringSet(startedTokens, []string{"first", "second"}) {
		t.Fatalf("started completions = %v, want first and second", startedTokens)
	}
	if got := maxActive.Load(); got != 2 {
		t.Fatalf("maximum concurrent submissions = %d, want 2", got)
	}

	close(releaseSecond)
	if completion := core.nextCompletion(t); string(completion.TaskToken) != "second" {
		t.Fatalf("first finished completion = %q, want second", completion.TaskToken)
	}
	close(releaseFirst)
	if completion := core.nextCompletion(t); string(completion.TaskToken) != "first" {
		t.Fatalf("second finished completion = %q, want first", completion.TaskToken)
	}
	cancel()
	waitForRun(t, runResult)
}

func TestRunIgnoresStaleCancelAfterCompletionAccepted(t *testing.T) {
	core := newFakeCore()
	release := make(chan struct{})
	attempted := make(chan struct{}, 1)
	core.onComplete = func(completion *corepb.ActivityTaskCompletion) error {
		if string(completion.TaskToken) == "token" {
			select {
			case attempted <- struct{}{}:
			default:
			}
			<-release
		}
		return nil
	}

	w := runTestWorker(t, core, 1)
	w.Register("complete", func() error { return nil })

	ctx, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- w.Run(ctx) }()

	core.sendStart(t, "token", "complete")
	waitForSignal(t, attempted, "the first completion attempt to start")
	firstAttempt := core.nextAttempt(t)
	if string(firstAttempt.TaskToken) != "token" {
		t.Fatalf("first completion attempt = %q, want token", firstAttempt.TaskToken)
	}
	core.sendCancel(t, "token", activitytask.ActivityCancelReason_CANCELLED)
	close(release)

	completion := core.nextCompletion(t)
	if _, ok := completion.Result.Status.(*activityresult.ActivityExecutionResult_Completed); !ok {
		t.Fatalf("completion status is %T, want completed", completion.Result.Status)
	}
	select {
	case duplicate := <-core.attempts:
		t.Fatalf("received duplicate completion attempt for %q", duplicate.TaskToken)
	case <-time.After(150 * time.Millisecond):
	}
	cancelRun()
	waitForRun(t, runResult)
}

func TestRunStartsNextActivityWhileCompletionSubmissionIsBlocked(t *testing.T) {
	core := newFakeCore()
	releaseFirstCompletion := make(chan struct{})
	firstCompletionAttempted := make(chan struct{}, 1)
	core.onComplete = func(completion *corepb.ActivityTaskCompletion) error {
		if string(completion.TaskToken) == "first" {
			select {
			case firstCompletionAttempted <- struct{}{}:
			default:
			}
			<-releaseFirstCompletion
		}
		return nil
	}

	w := runTestWorker(t, core, 1)
	started := make(chan string, 2)
	w.Register("complete", func(context.Context) error {
		started <- "started"
		return nil
	})

	ctx, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- w.Run(ctx) }()

	core.sendStart(t, "first", "complete")
	waitForString(t, started, "the first activity to start")
	waitForSignal(t, firstCompletionAttempted, "the first completion attempt to start")
	if string(core.nextAttempt(t).TaskToken) != "first" {
		t.Fatal("first completion attempt did not use the first token")
	}
	core.sendStart(t, "second", "complete")
	waitForString(t, started, "the second activity to start while the first completion is blocked")

	close(releaseFirstCompletion)
	secondAttempt := core.nextAttempt(t)
	if string(secondAttempt.TaskToken) != "second" {
		t.Fatalf("second completion attempt = %q, want second", secondAttempt.TaskToken)
	}
	cancelRun()
	waitForRun(t, runResult)
}

func TestRunDrainsAcceptedActivitiesAfterCompletionError(t *testing.T) {
	core := newFakeCore()
	allowFailure := make(chan struct{})
	secondAttempted := make(chan struct{}, 1)
	core.onComplete = func(completion *corepb.ActivityTaskCompletion) error {
		if string(completion.TaskToken) == "first" {
			<-allowFailure
			return errors.New("expected completion failure")
		}
		select {
		case secondAttempted <- struct{}{}:
		default:
		}
		return nil
	}

	w := runTestWorker(t, core, 2)
	w.Register("complete", func() error { return nil })

	runResult := make(chan error, 1)
	go func() { runResult <- w.Run(context.Background()) }()

	core.sendStart(t, "first", "complete")
	core.sendStart(t, "second", "complete")
	waitForSignal(t, secondAttempted, "the second accepted completion to attempt submission")
	close(allowFailure)

	err := waitForRunError(t, runResult)
	if !strings.Contains(err.Error(), "expected completion failure") {
		t.Fatalf("Run error = %v, want completion failure", err)
	}
	attemptedTokens := []string{
		string(core.nextAttempt(t).TaskToken),
		string(core.nextAttempt(t).TaskToken),
	}
	if !sameStringSet(attemptedTokens, []string{"first", "second"}) {
		t.Fatalf("completion attempts = %v, want first and second", attemptedTokens)
	}
}

func TestRunCancellationDrainsAcceptedActivityCompletions(t *testing.T) {
	core := newFakeCore()
	releaseFirst := make(chan struct{})
	secondAttempted := make(chan struct{}, 1)
	core.onComplete = func(completion *corepb.ActivityTaskCompletion) error {
		if string(completion.TaskToken) == "first" {
			<-releaseFirst
			return nil
		}
		select {
		case secondAttempted <- struct{}{}:
		default:
		}
		return nil
	}

	w := runTestWorker(t, core, 2)
	started := make(chan struct{}, 2)
	releaseHandlers := make(chan struct{})
	w.Register("complete", func(ctx context.Context) error {
		started <- struct{}{}
		<-releaseHandlers
		return nil
	})

	ctx, cancelRun := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- w.Run(ctx) }()

	core.sendStart(t, "first", "complete")
	core.sendStart(t, "second", "complete")
	waitForSignal(t, started, "first activity to start")
	waitForSignal(t, started, "second activity to start")
	close(releaseHandlers)
	waitForSignal(t, secondAttempted, "the second completion to attempt submission")
	cancelRun()
	close(releaseFirst)

	firstAttempt := core.nextAttempt(t)
	secondAttempt := core.nextAttempt(t)
	if !sameStringSet([]string{string(firstAttempt.TaskToken), string(secondAttempt.TaskToken)}, []string{"first", "second"}) {
		t.Fatalf("completion attempts = %q, %q, want first and second", firstAttempt.TaskToken, secondAttempt.TaskToken)
	}
	waitForRun(t, runResult)
}

func waitForString(t *testing.T, signal <-chan string, description string) string {
	t.Helper()
	select {
	case value := <-signal:
		return value
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return ""
	}
}

func sameStringSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	counts := make(map[string]int, len(want))
	for _, value := range want {
		counts[value]++
	}
	for _, value := range got {
		counts[value]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}
