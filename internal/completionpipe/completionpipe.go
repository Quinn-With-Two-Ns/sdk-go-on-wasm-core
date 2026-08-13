package completionpipe

import (
	"errors"
	"sync"
)

var (
	ErrAtCapacity  = errors.New("completion pipeline at capacity")
	ErrInvalidSize = errors.New("completion ticket weight must be positive")
)

// DefaultMaxOutstandingWeight caps retained completion payloads at 64 MiB.
const DefaultMaxOutstandingWeight = 64 << 20

// Ticket describes a single completion submission accepted by a worker loop.
type Ticket struct {
	Submit      func() error
	AfterSubmit func()
	Finalize    func()
	Weight      int
}

// Result reports the outcome of one submitted completion ticket.
type Result struct {
	Err error

	finalizeOnce sync.Once
	finalize     func()
}

// Finalize releases the ticket's outstanding slot and runs any caller-provided
// finalizer exactly once.
func (result *Result) Finalize() {
	if result == nil {
		return
	}
	result.finalizeOnce.Do(func() {
		if result.finalize != nil {
			result.finalize()
		}
	})
}

// Pipeline accepts completion tickets without blocking the caller, submits at
// most limit tickets concurrently, and reports completion results as they land.
type Pipeline struct {
	mu          sync.Mutex
	limit       int
	maxItems    int
	maxWeight   int
	active      int
	outstanding int
	totalWeight int
	queue       []queuedTicket
	results     chan *Result
}

type Option func(*Pipeline)

func WithMaxOutstanding(limit int) Option {
	return func(pipeline *Pipeline) {
		if limit < 1 {
			limit = 1
		}
		pipeline.maxItems = limit
	}
}

func WithMaxWeight(limit int) Option {
	return func(pipeline *Pipeline) {
		if limit < 1 {
			limit = 1
		}
		pipeline.maxWeight = limit
	}
}

type queuedTicket struct {
	ticket Ticket
	weight int
}

func New(limit int, options ...Option) *Pipeline {
	if limit < 1 {
		limit = 1
	}
	pipeline := &Pipeline{
		limit:     limit,
		maxItems:  limit * 2,
		maxWeight: DefaultMaxOutstandingWeight,
		results:   make(chan *Result, limit),
	}
	for _, option := range options {
		if option != nil {
			option(pipeline)
		}
	}
	if pipeline.maxItems < pipeline.limit {
		pipeline.maxItems = pipeline.limit
	}
	return pipeline
}

func (pipeline *Pipeline) Results() <-chan *Result {
	return pipeline.results
}

func (pipeline *Pipeline) Outstanding() int {
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()
	return pipeline.outstanding
}

func (pipeline *Pipeline) Enqueue(ticket Ticket) error {
	if ticket.Submit == nil {
		return errors.New("completion ticket submission is required")
	}
	weight, err := normalizedWeight(ticket)
	if err != nil {
		pipeline.reject(ticket)
		return err
	}

	pipeline.mu.Lock()
	overWeight := pipeline.maxWeight > 0 &&
		(weight > pipeline.maxWeight || pipeline.totalWeight > pipeline.maxWeight-weight)
	if pipeline.outstanding >= pipeline.maxItems || overWeight {
		pipeline.mu.Unlock()
		pipeline.reject(ticket)
		return ErrAtCapacity
	}
	pipeline.outstanding++
	pipeline.totalWeight += weight
	if pipeline.active < pipeline.limit {
		pipeline.active++
		pipeline.mu.Unlock()
		go pipeline.run(queuedTicket{ticket: ticket, weight: weight})
		return nil
	}
	pipeline.queue = append(pipeline.queue, queuedTicket{ticket: ticket, weight: weight})
	pipeline.mu.Unlock()
	return nil
}

func (pipeline *Pipeline) run(entry queuedTicket) {
	err := entry.ticket.Submit()
	if entry.ticket.AfterSubmit != nil {
		entry.ticket.AfterSubmit()
	}

	result := &Result{
		Err: err,
		finalize: func() {
			if entry.ticket.Finalize != nil {
				entry.ticket.Finalize()
			}
			pipeline.mu.Lock()
			pipeline.outstanding--
			pipeline.totalWeight -= entry.weight
			pipeline.mu.Unlock()
		},
	}

	pipeline.results <- result
	if next := pipeline.finishAndTakeNext(); next != nil {
		go pipeline.run(*next)
	}
}

func (pipeline *Pipeline) finishAndTakeNext() *queuedTicket {
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()
	pipeline.active--
	if len(pipeline.queue) == 0 {
		return nil
	}
	next := pipeline.queue[0]
	pipeline.queue[0] = queuedTicket{}
	pipeline.queue = pipeline.queue[1:]
	pipeline.active++
	return &next
}

func normalizedWeight(ticket Ticket) (int, error) {
	if ticket.Weight == 0 {
		return 1, nil
	}
	if ticket.Weight < 0 {
		return 0, ErrInvalidSize
	}
	return ticket.Weight, nil
}

func (pipeline *Pipeline) reject(ticket Ticket) {
	if ticket.Finalize != nil {
		ticket.Finalize()
	}
}
