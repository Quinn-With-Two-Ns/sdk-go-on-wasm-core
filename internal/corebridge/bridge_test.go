package corebridge

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	wasmbridgepb "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/wasmbridge"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/wasmhost"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestCompletionOperationIDsAreUnique(t *testing.T) {
	var bridge Bridge
	const count = 1_000
	ids := make(chan uint64, count)
	var workers sync.WaitGroup
	for range count {
		workers.Add(1)
		go func() {
			defer workers.Done()
			id, err := bridge.nextCompletionOperationID()
			if err != nil {
				t.Errorf("next completion operation ID: %v", err)
				return
			}
			ids <- id
		}()
	}
	workers.Wait()
	close(ids)

	seen := make(map[uint64]struct{}, count)
	for id := range ids {
		if id == 0 {
			t.Fatal("completion operation ID must be nonzero")
		}
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("duplicate completion operation ID %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("received %d IDs, want %d", len(seen), count)
	}
}

func TestCompletionOperationIDExhaustionDoesNotWrap(t *testing.T) {
	var bridge Bridge
	bridge.completionOperationID.Store(math.MaxUint64)
	if _, err := bridge.nextCompletionOperationID(); err == nil {
		t.Fatal("expected completion operation ID exhaustion error")
	}
	if got := bridge.completionOperationID.Load(); got != math.MaxUint64 {
		t.Fatalf("completion operation ID wrapped to %d", got)
	}
}

func TestDriveCompletionsCanOverlap(t *testing.T) {
	entered := make(chan int, 2)
	release := make(chan struct{})
	errCh := make(chan error, 2)

	for operation := 1; operation <= 2; operation++ {
		operation := operation
		go func() {
			firstTake := true
			errCh <- driveCompletion(
				context.Background(),
				func() error { return nil },
				func() (bool, error) {
					if firstTake {
						firstTake = false
						entered <- operation
						<-release
					}
					return true, nil
				},
			)
		}()
	}

	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("completion operations were serialized")
		}
	}
	close(release)
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatalf("drive completion: %v", err)
		}
	}
}

func TestDriveOperationStepRechecksAfterGRPCProgress(t *testing.T) {
	takes := 0
	pumps := 0
	result, ready, err := driveOperationStep(
		func() ([]byte, int32, error) {
			takes++
			if takes == 1 {
				return nil, bridgePending, nil
			}
			return []byte("ready"), 0, nil
		},
		func() (bool, error) {
			pumps++
			return true, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !ready || !bytes.Equal(result, []byte("ready")) {
		t.Fatalf("operation result = %q, ready = %v", result, ready)
	}
	if takes != 2 || pumps != 1 {
		t.Fatalf("takes = %d, pumps = %d; want 2 takes and 1 pump", takes, pumps)
	}
}

func TestDriveOperationStepReturnsPendingWithoutGRPCProgress(t *testing.T) {
	takes := 0
	result, ready, err := driveOperationStep(
		func() ([]byte, int32, error) {
			takes++
			return nil, bridgePending, nil
		},
		func() (bool, error) { return false, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if ready || result != nil {
		t.Fatalf("operation result = %q, ready = %v; want pending", result, ready)
	}
	if takes != 1 {
		t.Fatalf("takes = %d, want 1", takes)
	}
}

func TestBridgeWakeBroadcastsToAllWaiters(t *testing.T) {
	bridge := newTestBridge()
	first := bridge.currentWake()
	second := bridge.currentWake()
	bridge.signalWake()

	for index, wake := range []<-chan struct{}{first, second} {
		select {
		case <-wake:
		case <-time.After(time.Second):
			t.Fatalf("waiter %d was not woken", index)
		}
	}
	select {
	case <-bridge.currentWake():
		t.Fatal("a fresh wake generation was already closed")
	default:
	}
}

func TestBridgeWaitReturnsOnSignal(t *testing.T) {
	bridge := newTestBridge()
	wake := bridge.currentWake()
	result := make(chan error, 1)
	go func() { result <- bridge.waitForWake(context.Background(), wake, false) }()

	select {
	case err := <-result:
		t.Fatalf("wait returned before a signal: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	bridge.signalWake()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after signal")
	}
}

func TestBridgeWaitTimingHookRecordsElapsedWait(t *testing.T) {
	recorded := make(chan time.Duration, 1)
	restore := SetTimingHook(func(phase string, elapsed time.Duration) {
		if phase == "wait_for_wake" {
			recorded <- elapsed
		}
	})
	defer restore()

	bridge := newTestBridge()
	wake := bridge.currentWake()
	go func() {
		time.Sleep(10 * time.Millisecond)
		bridge.signalWake()
	}()
	if err := bridge.waitForWake(context.Background(), wake, false); err != nil {
		t.Fatal(err)
	}
	select {
	case elapsed := <-recorded:
		if elapsed < 5*time.Millisecond {
			t.Fatalf("recorded wait = %v, want a real elapsed duration", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("timing hook was not called")
	}
}

func TestBridgeWaitReturnsShutdownError(t *testing.T) {
	bridge := newTestBridge()
	wake := bridge.currentWake()
	result := make(chan error, 1)
	go func() { result <- bridge.waitForWake(context.Background(), wake, false) }()

	bridge.closing.Store(true)
	bridge.signalWake()

	select {
	case err := <-result:
		if !errors.Is(err, errBridgeShuttingDown) {
			t.Fatalf("wait returned %v, want shutdown error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after shutdown wake")
	}
}

func TestOwnerPanicPoisonsBridgeAndFutureCalls(t *testing.T) {
	bridge := newTestBridge()

	_, err := bridge.ownerCall(context.Background(), false, func(*bridgeOwner) (any, error) {
		panic("boom")
	})
	if err == nil || context.Cause(bridge.doneCtx) == nil {
		t.Fatalf("owner panic did not poison bridge: err=%v cause=%v", err, context.Cause(bridge.doneCtx))
	}
	if got := context.Cause(bridge.doneCtx); !errors.Is(got, err) {
		t.Fatalf("bridge cause = %v, want %v", got, err)
	}
	select {
	case <-bridge.grpcCtx.Done():
	default:
		t.Fatal("owner panic did not cancel in-flight gRPC calls")
	}

	_, laterErr := bridge.ownerCall(context.Background(), false, func(*bridgeOwner) (any, error) {
		return nil, nil
	})
	if !errors.Is(laterErr, err) {
		t.Fatalf("future owner call returned %v, want poison error %v", laterErr, err)
	}
}

func TestDeliverGRPCResponseAbortsOnShutdown(t *testing.T) {
	bridge := newTestBridge()
	bridge.grpcResponses = make(chan grpcResponse)
	result := make(chan bool, 1)

	go func() {
		result <- bridge.deliverGRPCResponse(grpcResponse{id: 1, response: []byte("late")})
	}()

	select {
	case delivered := <-result:
		t.Fatalf("delivery completed before shutdown: %v", delivered)
	case <-time.After(10 * time.Millisecond):
	}

	bridge.cancelDone(errBridgeShuttingDown)
	bridge.signalWake()

	select {
	case delivered := <-result:
		if delivered {
			t.Fatal("late response delivered after shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("late response delivery did not abort on shutdown")
	}
}

func TestNextBridgeBufferCapacity(t *testing.T) {
	tests := []struct {
		name     string
		current  int32
		required uint32
		want     int32
		wantErr  bool
	}{
		{name: "initial minimum", current: 0, required: 1, want: initialOutputCapacity},
		{name: "next power of two", current: initialOutputCapacity, required: initialOutputCapacity + 1, want: initialOutputCapacity * 2},
		{name: "exact match", current: initialOutputCapacity, required: initialOutputCapacity, want: initialOutputCapacity},
		{name: "caps at max", current: maxBridgeMessageSize / 2, required: maxBridgeMessageSize - 1, want: maxBridgeMessageSize},
		{name: "rejects zero", current: initialOutputCapacity, required: 0, wantErr: true},
		{name: "rejects over max", current: initialOutputCapacity, required: maxBridgeMessageSize + 1, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := nextBridgeBufferCapacity(test.current, test.required)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("next bridge buffer capacity: %v", err)
			}
			if got != test.want {
				t.Fatalf("next bridge buffer capacity = %d, want %d", got, test.want)
			}
		})
	}
}

func TestCheckedBridgeInputLenRejectsOversize(t *testing.T) {
	if _, err := checkedBridgeInputLen(maxBridgeMessageSize + 1); err == nil {
		t.Fatal("expected oversize length error")
	}
}

func TestDecodeGRPCRequestUsesSharedProtobufSchema(t *testing.T) {
	encoded, err := proto.Marshal(&wasmbridgepb.GrpcRequest{
		Id:      77,
		Service: "svc",
		Rpc:     "rpc",
		Metadata: []*wasmbridgepb.MetadataEntry{
			{Key: "request", Value: []byte("one")},
			{Key: "request", Value: []byte("again")},
			{Key: "request-bin", Value: []byte{0, 1, 0xff}},
		},
		Proto: []byte("proto"),
	})
	if err != nil {
		t.Fatal(err)
	}

	request, err := decodeGRPCRequest(encoded)
	if err != nil {
		t.Fatalf("decode gRPC request: %v", err)
	}
	if request.id != 77 || request.service != "svc" || request.rpc != "rpc" {
		t.Fatalf("decoded request = %#v", request)
	}
	if got := request.metadata.Get("request"); len(got) != 2 || got[0] != "one" || got[1] != "again" {
		t.Fatalf("repeated request metadata = %q", got)
	}
	if got := request.metadata.Get("request-bin"); len(got) != 1 || !bytes.Equal([]byte(got[0]), []byte{0, 1, 0xff}) {
		t.Fatalf("binary request metadata = %v", got)
	}
	if got := string(request.proto); got != "proto" {
		t.Fatalf("request proto = %q", got)
	}
}

func TestDecodeGRPCRequestRejectsMalformedProtobuf(t *testing.T) {
	if _, err := decodeGRPCRequest([]byte{0xff}); err == nil {
		t.Fatal("expected malformed gRPC request error")
	}
}

func TestDecodeGRPCRequestRejectsHostTransportOversize(t *testing.T) {
	if _, err := decodeGRPCRequest(make([]byte, maxHostTransportBytes+1)); err == nil {
		t.Fatal("expected oversized gRPC request error")
	}
}

func TestEncodeGRPCResponseUsesSharedProtobufSchema(t *testing.T) {
	encoded, err := encodeGRPCResponse(wasmhost.GRPCResponse{
		Code:    codes.ResourceExhausted,
		Message: "limited",
		Details: []byte{9, 8},
		Header: metadata.MD{
			"initial":     {"one", "again"},
			"initial-bin": {string([]byte{1, 2})},
		},
		Trailer: metadata.MD{"trailing": {"two"}},
		Proto:   []byte{1, 2, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wire wasmbridgepb.GrpcResponse
	if err := proto.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.StatusCode != int32(codes.ResourceExhausted) || wire.StatusMessage != "limited" {
		t.Fatalf("response status = (%d, %q)", wire.StatusCode, wire.StatusMessage)
	}
	if !bytes.Equal(wire.StatusDetails, []byte{9, 8}) || !bytes.Equal(wire.Proto, []byte{1, 2, 3}) {
		t.Fatalf("response payloads = details %v, proto %v", wire.StatusDetails, wire.Proto)
	}
	if len(wire.Headers) != 3 || wire.Headers[0].Key != "initial" || string(wire.Headers[0].Value) != "one" || string(wire.Headers[1].Value) != "again" {
		t.Fatalf("response headers = %#v", wire.Headers)
	}
	if wire.Headers[2].Key != "initial-bin" || !bytes.Equal(wire.Headers[2].Value, []byte{1, 2}) {
		t.Fatalf("binary response header = %#v", wire.Headers[2])
	}
	if len(wire.Trailers) != 1 || wire.Trailers[0].Key != "trailing" || string(wire.Trailers[0].Value) != "two" {
		t.Fatalf("response trailers = %#v", wire.Trailers)
	}
}

func TestEncodeGRPCResponseRejectsHostTransportOversize(t *testing.T) {
	_, err := encodeGRPCResponse(wasmhost.GRPCResponse{
		Code:  codes.OK,
		Proto: make([]byte, maxHostTransportBytes),
	})
	if err == nil {
		t.Fatal("expected oversized gRPC response error")
	}
}

func TestEagerActivityReservationsForCombinedModeDefaults(t *testing.T) {
	encoded, err := eagerActivityReservationsForMode(workerModeCombined, WorkflowOptions{})
	if err != nil {
		t.Fatalf("combined eager reservations: %v", err)
	}
	if encoded != defaultMaxEagerPerWFT {
		t.Fatalf("default eager reservations = %d, want %d", encoded, defaultMaxEagerPerWFT)
	}
}

func TestEagerActivityReservationsForCombinedModeRejectsNegativeValues(t *testing.T) {
	if _, err := eagerActivityReservationsForMode(workerModeCombined, WorkflowOptions{
		MaxEagerActivityReservationsPerWorkflowTask: -1,
	}); err == nil {
		t.Fatal("expected combined eager reservation validation error")
	}
}

func TestWorkerHeartbeatIntervalMillisPassesThroughSupportedValues(t *testing.T) {
	for _, want := range []int32{0, 1_000, 60_000} {
		got, err := workerHeartbeatIntervalMillis(WorkflowOptions{WorkerHeartbeatIntervalMillis: want})
		if err != nil {
			t.Fatalf("worker heartbeat interval %d: %v", want, err)
		}
		if got != want {
			t.Fatalf("worker heartbeat interval = %d, want %d", got, want)
		}
	}
}

func TestWorkerHeartbeatIntervalMillisRejectsNegativeValues(t *testing.T) {
	if _, err := workerHeartbeatIntervalMillis(WorkflowOptions{
		WorkerHeartbeatIntervalMillis: -1,
	}); err == nil {
		t.Fatal("expected worker heartbeat interval validation error")
	}
}

func TestRecordEagerActivityDispatchTimingRecordsEachReturnedActivity(t *testing.T) {
	recorded := make(chan string, 4)
	restore := SetTimingHook(func(phase string, elapsed time.Duration) {
		if elapsed != 0 {
			t.Fatalf("eager activity dispatch timing = %v, want zero marker duration", elapsed)
		}
		recorded <- phase
	})
	defer restore()

	encoded, err := proto.Marshal(&workflowservicepb.RespondWorkflowTaskCompletedResponse{
		ActivityTasks: []*workflowservicepb.PollActivityTaskQueueResponse{{}, {}},
	})
	if err != nil {
		t.Fatal(err)
	}

	recordEagerActivityDispatch(
		grpcRequest{service: workflowServiceName, rpc: respondWorkflowTaskRPC},
		wasmhost.GRPCResponse{Code: codes.OK, Proto: encoded},
	)

	for range 2 {
		select {
		case phase := <-recorded:
			if phase != "eager_activity_dispatch" {
				t.Fatalf("recorded phase = %q", phase)
			}
		case <-time.After(time.Second):
			t.Fatal("missing eager activity dispatch timing record")
		}
	}
	select {
	case phase := <-recorded:
		t.Fatalf("unexpected extra timing record %q", phase)
	default:
	}
}

func TestRecordEagerActivityDispatchReportsReturnedActivityCount(t *testing.T) {
	recorded := make(chan int, 1)
	restore := SetEagerActivityDispatchHook(func(count int) {
		recorded <- count
	})
	defer restore()

	encoded, err := proto.Marshal(&workflowservicepb.RespondWorkflowTaskCompletedResponse{
		ActivityTasks: []*workflowservicepb.PollActivityTaskQueueResponse{{}, {}, {}},
	})
	if err != nil {
		t.Fatal(err)
	}

	recordEagerActivityDispatch(
		grpcRequest{service: workflowServiceName, rpc: respondWorkflowTaskRPC},
		wasmhost.GRPCResponse{Code: codes.OK, Proto: encoded},
	)

	select {
	case count := <-recorded:
		if count != 3 {
			t.Fatalf("eager activity dispatch count = %d, want 3", count)
		}
	case <-time.After(time.Second):
		t.Fatal("missing eager activity dispatch count")
	}
}

func FuzzDecodeGRPCRequest(f *testing.F) {
	seed, err := proto.Marshal(&wasmbridgepb.GrpcRequest{Id: 77, Service: "svc", Rpc: "rpc", Proto: []byte("proto")})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > maxHostTransportBytes {
			return
		}
		_, _ = decodeGRPCRequest(encoded)
	})
}

func newTestBridge() *Bridge {
	doneCtx, cancelDone := context.WithCancelCause(context.Background())
	grpcCtx, cancelGRPC := context.WithCancel(context.Background())
	bridge := &Bridge{
		grpcResponses: make(chan grpcResponse, 1),
		ownerRequests: make(chan ownerRequest),
		doneCtx:       doneCtx,
		cancelDone:    cancelDone,
		grpcCtx:       grpcCtx,
		cancelGRPC:    cancelGRPC,
		wakeCh:        make(chan struct{}),
	}
	go bridge.ownerLoop()
	return bridge
}
