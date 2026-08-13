package converter

import (
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
)

type NilPayloadConverter struct{}

func NewNilPayloadConverter() *NilPayloadConverter {
	return &NilPayloadConverter{}
}

func (c *NilPayloadConverter) ToPayload(value any) (*commonpb.Payload, error) {
	if isInterfaceNil(value) {
		return newPayload(nil, c), nil
	}
	return nil, nil
}

func (*NilPayloadConverter) FromPayload(_ *commonpb.Payload, valuePtr any) error {
	originalValue := reflect.ValueOf(valuePtr)
	if originalValue.Kind() != reflect.Ptr || originalValue.IsNil() {
		return fmt.Errorf("type: %T: %w", valuePtr, ErrValuePtrIsNotPointer)
	}
	originalValue = originalValue.Elem()
	if !originalValue.CanSet() {
		return fmt.Errorf("type: %T: %w", valuePtr, ErrUnableToSetValue)
	}
	originalValue.Set(reflect.Zero(originalValue.Type()))
	return nil
}

func (*NilPayloadConverter) ToString(*commonpb.Payload) string { return "nil" }
func (*NilPayloadConverter) Encoding() string                  { return MetadataEncodingNil }
