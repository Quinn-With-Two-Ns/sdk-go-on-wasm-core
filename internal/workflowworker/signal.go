package workflowworker

import (
	"errors"
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
)

// ReceiveChannel receives the signals delivered to one signal name of a running workflow.
//
// Signals are buffered per signal name from the moment the run is initialized, so a channel
// obtained after a signal arrived still observes it. Each buffered signal is delivered to exactly
// one receiver, in the order Core delivered it.
type ReceiveChannel interface {
	// Receive blocks the calling workflow coroutine until a signal is available and decodes its
	// first input payload through valuePtr. A nil valuePtr discards the signal input.
	Receive(ctx *Context, valuePtr any) error
	// ReceiveAsync decodes a buffered signal without blocking and reports whether one was
	// available. A signal is consumed even when decoding it fails.
	ReceiveAsync(valuePtr any) (bool, error)
	// Len returns the number of buffered signals that no receiver has taken yet.
	Len() int
}

// GetSignalChannel returns the channel buffering signals named signalName.
//
// Repeated calls for the same name return the same channel, so separate workflow coroutines share
// one buffer instead of each observing every signal.
func GetSignalChannel(ctx *Context, signalName string) ReceiveChannel {
	ctx.requireRunning("GetSignalChannel")
	return ctx.execution.signalChannel(signalName)
}

type signalChannel struct {
	execution *workflowExecution
	name      string
	buffer    [][]*commonpb.Payload
	waiters   []*futureImpl
}

func (e *workflowExecution) signalChannel(name string) *signalChannel {
	if e.signalChannels == nil {
		e.signalChannels = make(map[string]*signalChannel)
	}
	channel := e.signalChannels[name]
	if channel == nil {
		channel = &signalChannel{execution: e, name: name}
		e.signalChannels[name] = channel
	}
	return channel
}

// deliverSignal buffers a signal, handing it directly to the coroutine that has been waiting
// longest when one is blocked in Receive.
func (c *signalChannel) deliverSignal(input []*commonpb.Payload) {
	for len(c.waiters) > 0 {
		waiter := c.waiters[0]
		c.waiters = c.waiters[1:]
		if waiter.ready {
			continue
		}
		waiter.set(input, nil)
		return
	}
	c.buffer = append(c.buffer, input)
}

func (c *signalChannel) Receive(ctx *Context, valuePtr any) error {
	if ctx == nil || ctx.execution != c.execution {
		return errors.New("signal Receive requires its workflow context")
	}
	if err := validateSignalTarget(valuePtr); err != nil {
		return err
	}
	if input, ok := c.take(); ok {
		return c.decode(input, valuePtr)
	}

	waiter := newFuture(c.execution)
	c.waiters = append(c.waiters, waiter)
	for !waiter.ready {
		if err := ctx.blockOn(waiter); err != nil {
			c.removeWaiter(waiter)
			return err
		}
	}
	input, _ := waiter.value.([]*commonpb.Payload)
	return c.decode(input, valuePtr)
}

func (c *signalChannel) ReceiveAsync(valuePtr any) (bool, error) {
	if err := validateSignalTarget(valuePtr); err != nil {
		return false, err
	}
	input, ok := c.take()
	if !ok {
		return false, nil
	}
	return true, c.decode(input, valuePtr)
}

func (c *signalChannel) Len() int { return len(c.buffer) }

func (c *signalChannel) take() ([]*commonpb.Payload, bool) {
	if len(c.buffer) == 0 {
		return nil, false
	}
	input := c.buffer[0]
	c.buffer = c.buffer[1:]
	return input, true
}

func (c *signalChannel) removeWaiter(waiter *futureImpl) {
	for i, candidate := range c.waiters {
		if candidate == waiter {
			c.waiters = append(c.waiters[:i:i], c.waiters[i+1:]...)
			return
		}
	}
}

// decode converts the first signal input payload. A signal sent without input leaves the target at
// its zero value, and additional inputs beyond the first are not decoded.
func (c *signalChannel) decode(input []*commonpb.Payload, valuePtr any) error {
	if valuePtr == nil || len(input) == 0 {
		return nil
	}
	if err := c.execution.payloadConverter.FromPayload(input[0], valuePtr); err != nil {
		return fmt.Errorf("decode signal %q: %w", c.name, err)
	}
	return nil
}

func validateSignalTarget(valuePtr any) error {
	if valuePtr == nil {
		return nil
	}
	value := reflect.ValueOf(valuePtr)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return errors.New("signal result must be a non-nil pointer")
	}
	return nil
}
