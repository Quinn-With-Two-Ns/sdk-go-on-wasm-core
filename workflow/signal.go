package workflow

import (
	"fmt"

	internalworkflow "github.com/temporalio/sdk-go-on-wasm-core/internal/workflowworker"
)

// UntypedReceiveChannel receives signals decoded through a caller-provided pointer.
type UntypedReceiveChannel = internalworkflow.ReceiveChannel

// ReceiveChannel receives signals sent to one signal name as values of type T.
// Signals are delivered once, in order, and repeated channel lookups by name share a buffer.
type ReceiveChannel[T any] struct {
	channel UntypedReceiveChannel
}

// GetSignalChannel returns the typed channel for signals named signalName.
// Signals without input decode as the zero value of T. Only the first signal argument is decoded.
func GetSignalChannel[T any](ctx *Context, signalName string) ReceiveChannel[T] {
	return ReceiveChannel[T]{channel: internalworkflow.GetSignalChannel(ctx, signalName)}
}

// GetSignalChannelUntyped returns the pointer-decoded channel for signals named signalName.
func GetSignalChannelUntyped(ctx *Context, signalName string) UntypedReceiveChannel {
	return internalworkflow.GetSignalChannel(ctx, signalName)
}

// Receive blocks the calling workflow coroutine until a signal arrives and returns its value.
// Other workflow coroutines remain runnable while this coroutine is blocked.
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
// A signal that fails decoding is still consumed and returns ok true with the decoding error.
func (c ReceiveChannel[T]) ReceiveAsync() (value T, ok bool, err error) {
	if c.channel == nil {
		return value, false, fmt.Errorf("workflow signal channel is not initialized")
	}
	ok, err = c.channel.ReceiveAsync(&value)
	if err != nil {
		var zero T
		return zero, ok, err
	}
	return value, ok, nil
}

// Len returns the number of signals buffered for this channel.
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
