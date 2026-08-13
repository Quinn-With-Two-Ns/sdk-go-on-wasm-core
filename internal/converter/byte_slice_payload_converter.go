package converter

import (
	"encoding/base64"
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
)

type ByteSlicePayloadConverter struct{}

func NewByteSlicePayloadConverter() *ByteSlicePayloadConverter {
	return &ByteSlicePayloadConverter{}
}

func (c *ByteSlicePayloadConverter) ToPayload(value any) (*commonpb.Payload, error) {
	if valueBytes, ok := value.([]byte); ok {
		return newPayload(valueBytes, c), nil
	}
	return nil, nil
}

func (*ByteSlicePayloadConverter) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	rv := reflect.ValueOf(valuePtr)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("type: %T: %w", valuePtr, ErrValuePtrIsNotPointer)
	}
	v := rv.Elem()
	switch {
	case v.Kind() == reflect.Interface:
		v.Set(reflect.ValueOf(payload.GetData()))
	case v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8:
		v.SetBytes(payload.GetData())
	default:
		return fmt.Errorf("type %T: %w", valuePtr, ErrTypeIsNotByteSlice)
	}
	return nil
}

func (c *ByteSlicePayloadConverter) ToString(payload *commonpb.Payload) string {
	var value []byte
	if err := c.FromPayload(payload, &value); err != nil {
		return err.Error()
	}
	return base64.RawStdEncoding.EncodeToString(value)
}

func (*ByteSlicePayloadConverter) Encoding() string { return MetadataEncodingBinary }
