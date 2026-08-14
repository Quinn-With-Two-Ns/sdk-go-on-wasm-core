package workflow

import (
	"fmt"

	internalworkflow "github.com/temporalio/sdk-go-on-wasm-core/internal/workflowworker"
)

// UntypedReceiveChannel receives signals decoded through a caller-provided pointer.
type UntypedReceiveChannel = internalworkflow.ReceiveChannel

// ReceiveChannel receives the signals sent to one signal name of the running workflow as values of
// type T.
//
// Signals are buffered per signal name from the moment the run is initialized, so a channel
// obtained after a signal arrived still observes it. Each signal is delivered to exactly one
// receiver, in the order the server recorded it. Channels for the same name share one buffer, so
// separate workflow coroutines divide the signals between them rather than each observing all.
type ReceiveChannel[T any] struct {
	channel UntypedReceiveChannel
}

// GetSignalChannel returns the channel of signals named signalName carrying values of type T.
//
// Signals sent with no input decode as the zero value of T; only the first signal argument is
// decoded.
func GetSignalChannel[T any](ctx *Context, signalName string) ReceiveChannel[T] {
	return ReceiveChannel[T]{channel: internalworkflow.GetSignalChannel(ctx, signalName)}
}

// GetSignalChannelUntyped returns the channel of signals named signalName.
//
// Use this dynamic escape hatch when the signal payload type is only known at runtime. Its values
// are decoded by passing a pointer to UntypedReceiveChannel.Receive or ReceiveAsync.
func GetSignalChannelUntyped(ctx *Context, signalName string) UntypedReceiveChannel {
	return internalworkflow.GetSignalChannel(ctx, signalName)
}

// Receive blocks the calling workflow coroutine until a signal arrives and returns its value.
//
// Only this coroutine blocks; other workflow coroutines continue to run. Await a signal alongside
// futures by receiving in a coroutine started with Go and completing a Settable with the result.
func (c ReceiveChannel[T]) Receive(ctx *Context) (T, error) {
	var value T
	if c.channel == nil {
		return value, fmt.Errorf("workflow signal channel is not initialized")
	}
	if err := c.channel.Receive(ctx, &value); err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}

// ReceiveAsync returns a buffered signal without blocking and reports whether one was available.
//
// A signal is consumed even when decoding it fails, which is reported as a non-nil error together
// with ok true.
func (c ReceiveChannel[T]) ReceiveAsync() (T, bool, error) {
	var value T
	if c.channel == nil {
		return value, false, fmt.Errorf("workflow signal channel is not initialized")
	}
	ok, err := c.channel.ReceiveAsync(&value)
	if err != nil {
		var zero T
		return zero, ok, err
	}
	return value, ok, nil
}

// Len returns the number of buffered signals that no receiver has taken yet.
func (c ReceiveChannel[T]) Len() int {
	if c.channel == nil {
		return 0
	}
	return c.channel.Len()
}

// Untyped returns the pointer-decoded form of the signal channel.
func (c ReceiveChannel[T]) Untyped() UntypedReceiveChannel {
	return c.channel
}
