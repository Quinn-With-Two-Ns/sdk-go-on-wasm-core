package workflow

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

type stubReceiveChannel struct {
	value       any
	err         error
	available   bool
	buffered    int
	receiveCall bool
	asyncCall   bool
	target      any
}

func (c *stubReceiveChannel) Receive(_ *Context, valuePtr any) error {
	c.receiveCall = true
	c.target = valuePtr
	if c.err != nil {
		return c.err
	}
	c.assign(valuePtr)
	return nil
}

func (c *stubReceiveChannel) ReceiveAsync(valuePtr any) (bool, error) {
	c.asyncCall = true
	c.target = valuePtr
	if !c.available {
		return false, nil
	}
	if c.err != nil {
		return true, c.err
	}
	c.assign(valuePtr)
	return true, nil
}

func (c *stubReceiveChannel) Len() int { return c.buffered }

func (c *stubReceiveChannel) assign(valuePtr any) {
	if valuePtr == nil || c.value == nil {
		return
	}
	reflect.ValueOf(valuePtr).Elem().Set(reflect.ValueOf(c.value))
}

func TestSignalChannelReceiveReturnsTypedValue(t *testing.T) {
	untyped := &stubReceiveChannel{value: "Temporal", buffered: 2}
	channel := ReceiveChannel[string]{channel: untyped}

	value, err := channel.Receive(nil)
	if err != nil {
		t.Fatal(err)
	}
	if value != "Temporal" || !untyped.receiveCall {
		t.Fatalf("Receive() = %q, receive called = %t", value, untyped.receiveCall)
	}
	if _, ok := untyped.target.(*string); !ok {
		t.Fatalf("Receive() decoded through %T, want *string", untyped.target)
	}
	if channel.Len() != 2 || channel.Untyped() != untyped {
		t.Fatalf("typed channel did not preserve its buffer length or untyped form")
	}
}

func TestSignalChannelReceiveReturnsZeroValueOnError(t *testing.T) {
	want := errors.New("decode failed")
	value, err := (ReceiveChannel[string]{channel: &stubReceiveChannel{value: "Temporal", err: want}}).Receive(nil)
	if !errors.Is(err, want) || value != "" {
		t.Fatalf("Receive() = (%q, %v), want zero value and %v", value, err, want)
	}
}

func TestSignalChannelReceiveAsyncReportsAvailability(t *testing.T) {
	empty := &stubReceiveChannel{value: "Temporal"}
	value, ok, err := (ReceiveChannel[string]{channel: empty}).ReceiveAsync()
	if err != nil || ok || value != "" {
		t.Fatalf("ReceiveAsync() on an empty channel = (%q, %t, %v)", value, ok, err)
	}
	if !empty.asyncCall {
		t.Fatal("ReceiveAsync() did not reach the untyped channel")
	}

	buffered := &stubReceiveChannel{value: "Temporal", available: true}
	value, ok, err = (ReceiveChannel[string]{channel: buffered}).ReceiveAsync()
	if err != nil || !ok || value != "Temporal" {
		t.Fatalf("ReceiveAsync() on a buffered channel = (%q, %t, %v)", value, ok, err)
	}
}

func TestSignalChannelReceiveAsyncReportsConsumedSignalOnDecodeError(t *testing.T) {
	want := errors.New("decode failed")
	channel := ReceiveChannel[string]{channel: &stubReceiveChannel{available: true, err: want}}
	value, ok, err := channel.ReceiveAsync()
	if !errors.Is(err, want) || !ok || value != "" {
		t.Fatalf("ReceiveAsync() = (%q, %t, %v), want a consumed signal and %v", value, ok, err, want)
	}
}

func TestUninitializedSignalChannelReportsAnError(t *testing.T) {
	var channel ReceiveChannel[string]
	if _, err := channel.Receive(nil); err == nil || !strings.Contains(err.Error(), "not initialized") {
		t.Fatalf("Receive() error = %v, want an initialization error", err)
	}
	if _, ok, err := channel.ReceiveAsync(); ok || err == nil || !strings.Contains(err.Error(), "not initialized") {
		t.Fatalf("ReceiveAsync() = (%t, %v), want an initialization error", ok, err)
	}
	if channel.Len() != 0 || channel.Untyped() != nil {
		t.Fatalf("uninitialized channel reported %d buffered signals", channel.Len())
	}
}
