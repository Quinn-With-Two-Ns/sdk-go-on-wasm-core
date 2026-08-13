package converter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/temporalproto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type ProtoJSONPayloadConverter struct {
	marshalOptions           protojson.MarshalOptions
	unmarshalOptions         protojson.UnmarshalOptions
	temporalUnmarshalOptions temporalproto.CustomJSONUnmarshalOptions
	options                  ProtoJSONPayloadConverterOptions
}

type ProtoJSONPayloadConverterOptions struct {
	ExcludeProtobufMessageTypes bool
	AllowUnknownFields          bool
	UseProtoNames               bool
	UseEnumNumbers              bool
	EmitUnpopulated             bool
	LegacyTemporalProtoCompat   bool
}

var jsonNil, _ = json.Marshal(nil)

func NewProtoJSONPayloadConverter() *ProtoJSONPayloadConverter {
	return NewProtoJSONPayloadConverterWithOptions(ProtoJSONPayloadConverterOptions{})
}

func NewProtoJSONPayloadConverterWithOptions(options ProtoJSONPayloadConverterOptions) *ProtoJSONPayloadConverter {
	return &ProtoJSONPayloadConverter{
		marshalOptions: protojson.MarshalOptions{
			UseProtoNames:   options.UseProtoNames,
			UseEnumNumbers:  options.UseEnumNumbers,
			EmitUnpopulated: options.EmitUnpopulated,
		},
		unmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: options.AllowUnknownFields},
		temporalUnmarshalOptions: temporalproto.CustomJSONUnmarshalOptions{
			DiscardUnknown: options.AllowUnknownFields,
		},
		options: options,
	}
}

func (c *ProtoJSONPayloadConverter) ToPayload(value any) (*commonpb.Payload, error) {
	if isInterfaceNil(value) {
		return newPayload(jsonNil, c), nil
	}
	for builtPointer := false; ; builtPointer = true {
		if message, ok := value.(proto.Message); ok {
			data, err := c.marshalOptions.Marshal(message)
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

func (c *ProtoJSONPayloadConverter) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	originalValue := reflect.ValueOf(valuePtr)
	if originalValue.Kind() != reflect.Ptr || originalValue.IsNil() {
		return fmt.Errorf("type: %T: %w", valuePtr, ErrValuePtrIsNotPointer)
	}
	originalValue = originalValue.Elem()
	if !originalValue.CanSet() {
		return fmt.Errorf("type: %T: %w", valuePtr, ErrUnableToSetValue)
	}
	if bytes.Equal(payload.GetData(), jsonNil) {
		originalValue.Set(reflect.Zero(originalValue.Type()))
		return nil
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

	var err error
	if c.options.LegacyTemporalProtoCompat {
		err = c.temporalUnmarshalOptions.Unmarshal(payload.GetData(), message)
	} else {
		err = c.unmarshalOptions.Unmarshal(payload.GetData(), message)
	}
	if originalValue.Kind() != reflect.Ptr {
		originalValue.Set(value.Elem())
	}
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnableToDecode, err)
	}
	return nil
}

func (*ProtoJSONPayloadConverter) ToString(payload *commonpb.Payload) string {
	return string(payload.GetData())
}

func (*ProtoJSONPayloadConverter) Encoding() string { return MetadataEncodingProtoJSON }

func (c *ProtoJSONPayloadConverter) ExcludeProtobufMessageTypes() bool {
	return c.options.ExcludeProtobufMessageTypes
}
