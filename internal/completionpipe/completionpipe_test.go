package completionpipe

import (
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipelineQueuesWithoutBlockingAndRespectsLimit(t *testing.T) {
	pipeline := New(2, WithMaxOutstanding(3))
	release := []chan struct{}{
		make(chan struct{}),
		make(chan struct{}),
		make(chan struct{}),
	}
	started := make(chan int, 3)
	for i := 0; i < 3; i++ {
		index := i
		if err := pipeline.Enqueue(Ticket{
			Submit: func() error {
				started <- index
				<-release[index]
				return nil
			},
		}); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", index, err)
		}
	}

	if got := pipeline.Outstanding(); got != 3 {
		t.Fatalf("Outstanding() = %d, want 3", got)
	}
	first := waitForStart(t, started, "first submission to start")
	second := waitForStart(t, started, "second submission to start")
	if !sameSet(first, second, 0, 1) {
		t.Fatalf("started submissions = %d, %d, want 0 and 1", first, second)
	}
	select {
	case unexpected := <-started:
		t.Fatalf("submission %d started before a slot was released", unexpected)
	case <-time.After(150 * time.Millisecond):
	}

	close(release[first])
	result := waitForResult(t, pipeline.Results(), "first submission result")
	if result.Err != nil {
		t.Fatalf("first result error = %v", result.Err)
	}
	result.Finalize()

	if third := waitForStart(t, started, "queued submission to start"); third != 2 {
		t.Fatalf("queued submission start = %d, want 2", third)
	}

	close(release[second])
	waitForResult(t, pipeline.Results(), "second submission result").Finalize()
	close(release[2])
	waitForResult(t, pipeline.Results(), "third submission result").Finalize()

	if got := pipeline.Outstanding(); got != 0 {
		t.Fatalf("Outstanding() after finalization = %d, want 0", got)
	}
}

func TestPipelineRejectsAtCapacityWithoutSubmittingAndRunsFinalizerOnce(t *testing.T) {
	pipeline := New(1, WithMaxOutstanding(2))
	release := make(chan struct{})
	if err := pipeline.Enqueue(Ticket{
		Submit: func() error {
			<-release
			return nil
		},
	}); err != nil {
		t.Fatalf("first Enqueue() error = %v", err)
	}
	if err := pipeline.Enqueue(Ticket{
		Submit: func() error { return nil },
	}); err != nil {
		t.Fatalf("second Enqueue() error = %v", err)
	}

	var submitCalls atomic.Int32
	var afterSubmitCalls atomic.Int32
	var finalizeCalls atomic.Int32
	start := time.Now()
	err := pipeline.Enqueue(Ticket{
		Submit: func() error {
			submitCalls.Add(1)
			return nil
		},
		AfterSubmit: func() {
			afterSubmitCalls.Add(1)
		},
		Finalize: func() {
			finalizeCalls.Add(1)
		},
	})
	if !errors.Is(err, ErrAtCapacity) {
		t.Fatalf("rejected Enqueue() error = %v, want %v", err, ErrAtCapacity)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("rejected Enqueue() blocked for %v", elapsed)
	}
	if got := submitCalls.Load(); got != 0 {
		t.Fatalf("rejected Submit called %d times, want 0", got)
	}
	if got := afterSubmitCalls.Load(); got != 0 {
		t.Fatalf("rejected AfterSubmit called %d times, want 0", got)
	}
	if got := finalizeCalls.Load(); got != 1 {
		t.Fatalf("rejected Finalize called %d times, want 1", got)
	}
	if got := pipeline.Outstanding(); got != 2 {
		t.Fatalf("Outstanding() after rejection = %d, want 2", got)
	}
	select {
	case result := <-pipeline.Results():
		t.Fatalf("unexpected result for rejected ticket: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	waitForResult(t, pipeline.Results(), "first accepted result").Finalize()
	waitForResult(t, pipeline.Results(), "second accepted result").Finalize()
	if got := pipeline.Outstanding(); got != 0 {
		t.Fatalf("Outstanding() after draining accepted work = %d, want 0", got)
	}
}

func TestPipelineRejectsWhenWeightBudgetWouldBeExceededUntilFinalize(t *testing.T) {
	pipeline := New(1, WithMaxOutstanding(4), WithMaxWeight(5))
	release := make(chan struct{})
	if err := pipeline.Enqueue(Ticket{
		Weight: 3,
		Submit: func() error {
			<-release
			return nil
		},
	}); err != nil {
		t.Fatalf("first weighted Enqueue() error = %v", err)
	}
	if err := pipeline.Enqueue(Ticket{
		Weight: 2,
		Submit: func() error { return nil },
	}); err != nil {
		t.Fatalf("second weighted Enqueue() error = %v", err)
	}

	var rejectedFinalizes atomic.Int32
	err := pipeline.Enqueue(Ticket{
		Weight: 1,
		Submit: func() error { return nil },
		Finalize: func() {
			rejectedFinalizes.Add(1)
		},
	})
	if !errors.Is(err, ErrAtCapacity) {
		t.Fatalf("weight-limited Enqueue() error = %v, want %v", err, ErrAtCapacity)
	}
	if got := rejectedFinalizes.Load(); got != 1 {
		t.Fatalf("rejected weighted Finalize called %d times, want 1", got)
	}
	if got := pipeline.Outstanding(); got != 2 {
		t.Fatalf("Outstanding() with full weight budget = %d, want 2", got)
	}

	close(release)
	first := waitForResult(t, pipeline.Results(), "first weighted result")
	second := waitForResult(t, pipeline.Results(), "second weighted result")

	err = pipeline.Enqueue(Ticket{
		Weight: 1,
		Submit: func() error { return nil },
	})
	if !errors.Is(err, ErrAtCapacity) {
		t.Fatalf("Enqueue() before Finalize released weight error = %v, want %v", err, ErrAtCapacity)
	}

	first.Finalize()
	if err := pipeline.Enqueue(Ticket{
		Weight: 1,
		Submit: func() error { return nil },
	}); err != nil {
		t.Fatalf("Enqueue() after Finalize released weight error = %v", err)
	}
	second.Finalize()
	waitForResult(t, pipeline.Results(), "post-finalize weighted result").Finalize()
	if got := pipeline.Outstanding(); got != 0 {
		t.Fatalf("Outstanding() after weighted finalization = %d, want 0", got)
	}
}

func TestPipelineAppliesDefaultWeightBudget(t *testing.T) {
	pipeline := New(1)
	var finalized atomic.Int32
	err := pipeline.Enqueue(Ticket{
		Weight: DefaultMaxOutstandingWeight + 1,
		Submit: func() error { return nil },
		Finalize: func() {
			finalized.Add(1)
		},
	})
	if !errors.Is(err, ErrAtCapacity) {
		t.Fatalf("Enqueue() error = %v, want %v", err, ErrAtCapacity)
	}
	if got := finalized.Load(); got != 1 {
		t.Fatalf("rejected Finalize called %d times, want 1", got)
	}
}

func TestPipelineWeightAccountingDoesNotOverflow(t *testing.T) {
	pipeline := New(1, WithMaxWeight(math.MaxInt))
	release := make(chan struct{})
	if err := pipeline.Enqueue(Ticket{
		Weight: math.MaxInt - 1,
		Submit: func() error {
			<-release
			return nil
		},
	}); err != nil {
		t.Fatalf("first Enqueue() error = %v", err)
	}
	err := pipeline.Enqueue(Ticket{
		Weight: 2,
		Submit: func() error { return nil },
	})
	if !errors.Is(err, ErrAtCapacity) {
		t.Fatalf("overflowing Enqueue() error = %v, want %v", err, ErrAtCapacity)
	}
	close(release)
	waitForResult(t, pipeline.Results(), "large weighted result").Finalize()
}

func TestResultFinalizeRunsTicketFinalizerOnceAndReleasesOutstanding(t *testing.T) {
	pipeline := New(1)
	var finalized atomic.Int32
	if err := pipeline.Enqueue(Ticket{
		Submit: func() error { return errors.New("boom") },
		Finalize: func() {
			finalized.Add(1)
		},
	}); err != nil {
		t.Fatal(err)
	}

	result := waitForResult(t, pipeline.Results(), "submission result")
	if result.Err == nil || result.Err.Error() != "boom" {
		t.Fatalf("result error = %v, want boom", result.Err)
	}
	if got := pipeline.Outstanding(); got != 1 {
		t.Fatalf("Outstanding() before Finalize = %d, want 1", got)
	}

	result.Finalize()
	result.Finalize()

	if got := finalized.Load(); got != 1 {
		t.Fatalf("Finalize() called ticket finalizer %d times, want 1", got)
	}
	if got := pipeline.Outstanding(); got != 0 {
		t.Fatalf("Outstanding() after Finalize = %d, want 0", got)
	}
}

func waitForStart(t *testing.T, started <-chan int, description string) int {
	t.Helper()
	select {
	case value := <-started:
		return value
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return 0
	}
}

func waitForResult(t *testing.T, results <-chan *Result, description string) *Result {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return nil
	}
}

func sameSet(gotFirst, gotSecond int, wantFirst, wantSecond int) bool {
	return (gotFirst == wantFirst && gotSecond == wantSecond) || (gotFirst == wantSecond && gotSecond == wantFirst)
}
