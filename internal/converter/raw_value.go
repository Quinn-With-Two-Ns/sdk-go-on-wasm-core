package converter

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
)

// RawValue passes an already serialized payload through a PayloadConverter unchanged.
// The worker's payload codec still transforms it at the Core boundary.
type RawValue struct {
	payload *commonpb.Payload
}

func NewRawValue(payload *commonpb.Payload) RawValue {
	return RawValue{payload: payload}
}

func (v RawValue) Payload() *commonpb.Payload {
	return v.payload
}

func (RawValue) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("RawValue is not JSON serializable")
}

func (*RawValue) UnmarshalJSON([]byte) error {
	return fmt.Errorf("RawValue is not JSON serializable")
}
