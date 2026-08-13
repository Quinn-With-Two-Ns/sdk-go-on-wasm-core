package converter

import (
	"encoding/base64"
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

type ProtoPayloadConverter struct {
	options ProtoPayloadConverterOptions
}

type ProtoPayloadConverterOptions struct {
	ExcludeProtobufMessageTypes bool
}

func NewProtoPayloadConverter() *ProtoPayloadConverter {
	return &ProtoPayloadConverter{}
}

func NewProtoPayloadConverterWithOptions(options ProtoPayloadConverterOptions) *ProtoPayloadConverter {
	return &ProtoPayloadConverter{options: options}
}

func (c *ProtoPayloadConverter) ToPayload(value any) (*commonpb.Payload, error) {
	for builtPointer := false; ; builtPointer = true {
		if message, ok := value.(proto.Message); ok {
			data, err := proto.Marshal(message)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrUnableToEncode, err)
			}
			return newProtoPayload(data, c, string(message.ProtoReflect().Descriptor().FullName())), nil
		}
		if builtPointer {
			return nil, nil
		}
		value = pointerTo(value).Interface()
	}
}

func (*ProtoPayloadConverter) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	originalValue := reflect.ValueOf(valuePtr)
	if originalValue.Kind() != reflect.Ptr || originalValue.IsNil() {
		return fmt.Errorf("type: %T: %w", valuePtr, ErrValuePtrIsNotPointer)
	}
	originalValue = originalValue.Elem()
	if !originalValue.CanSet() {
		return fmt.Errorf("type: %T: %w", valuePtr, ErrUnableToSetValue)
	}
	if originalValue.Kind() == reflect.Interface {
		return fmt.Errorf("value type: %s: %w", originalValue.Type(), ErrValuePtrMustConcreteType)
	}

	value := originalValue
	if originalValue.Kind() != reflect.Ptr {
		value = pointerTo(originalValue.Interface())
	}
	message, ok := value.Interface().(proto.Message)
	if !ok {
		return fmt.Errorf("type: %T: %w", value.Interface(), ErrTypeNotImplementProtoMessage)
	}
	if originalValue.Kind() == reflect.Ptr && originalValue.IsNil() {
		value = newOfSameType(originalValue)
		message = value.Interface().(proto.Message)
	}

	err := proto.Unmarshal(payload.GetData(), message)
	if originalValue.Kind() != reflect.Ptr {
		originalValue.Set(value.Elem())
	}
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnableToDecode, err)
	}
	return nil
}

func (*ProtoPayloadConverter) ToString(payload *commonpb.Payload) string {
	return base64.RawStdEncoding.EncodeToString(payload.GetData())
}

func (*ProtoPayloadConverter) Encoding() string { return MetadataEncodingProto }

func (c *ProtoPayloadConverter) ExcludeProtobufMessageTypes() bool {
	return c.options.ExcludeProtobufMessageTypes
}
