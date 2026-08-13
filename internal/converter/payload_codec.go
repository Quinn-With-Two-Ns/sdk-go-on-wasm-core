package converter

import (
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	payloadMessageName          = protoreflect.FullName("temporal.api.common.v1.Payload")
	searchAttributesMessageName = protoreflect.FullName("temporal.api.common.v1.SearchAttributes")
)

// PayloadCodec transforms serialized payloads at the SDK/Core boundary.
// Implementations must not mutate the input payloads.
type PayloadCodec interface {
	Encode([]*commonpb.Payload) ([]*commonpb.Payload, error)
	Decode([]*commonpb.Payload) ([]*commonpb.Payload, error)
}

type noopPayloadCodec struct{}

func (noopPayloadCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return payloads, nil
}

func (noopPayloadCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return payloads, nil
}

// NewNoopPayloadCodec returns a codec that leaves payloads unchanged.
func NewNoopPayloadCodec() PayloadCodec {
	return noopPayloadCodec{}
}

// EncodePayloads applies codec encoding to every eligible payload field in message.
func EncodePayloads(message proto.Message, codec PayloadCodec) error {
	if codec == nil {
		return errors.New("payload codec is nil")
	}
	return transformPayloads(message, codec.Encode)
}

// DecodePayloads applies codec decoding to every eligible payload field in message.
func DecodePayloads(message proto.Message, codec PayloadCodec) error {
	if codec == nil {
		return errors.New("payload codec is nil")
	}
	return transformPayloads(message, codec.Decode)
}

type payloadTransform func([]*commonpb.Payload) ([]*commonpb.Payload, error)

func transformPayloads(message proto.Message, transform payloadTransform) error {
	if message == nil || !message.ProtoReflect().IsValid() {
		return errors.New("payload message is nil")
	}
	return transformMessagePayloads(message.ProtoReflect(), transform)
}

func transformMessagePayloads(message protoreflect.Message, transform payloadTransform) error {
	if message.Descriptor().FullName() == searchAttributesMessageName {
		return nil
	}

	fields := message.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if !message.Has(field) {
			continue
		}
		if err := transformFieldPayloads(message, field, transform); err != nil {
			return fmt.Errorf("%s: %w", field.FullName(), err)
		}
	}
	return nil
}

func transformFieldPayloads(
	message protoreflect.Message,
	field protoreflect.FieldDescriptor,
	transform payloadTransform,
) error {
	if field.IsMap() {
		return transformMapPayloads(message.Get(field).Map(), field.MapValue(), transform)
	}
	if field.IsList() {
		list := message.Get(field).List()
		if field.Message() == nil {
			return nil
		}
		if field.Message().FullName() == payloadMessageName {
			return transformPayloadList(list, transform)
		}
		for i := 0; i < list.Len(); i++ {
			if err := transformMessagePayloads(list.Get(i).Message(), transform); err != nil {
				return err
			}
		}
		return nil
	}
	if field.Message() == nil {
		return nil
	}
	if field.Message().FullName() == payloadMessageName {
		converted, err := transformSinglePayload(payloadFromMessage(message.Get(field).Message()), transform)
		if err != nil {
			return err
		}
		message.Set(field, protoreflect.ValueOfMessage(converted.ProtoReflect()))
		return nil
	}
	return transformMessagePayloads(message.Get(field).Message(), transform)
}

func transformMapPayloads(
	values protoreflect.Map,
	valueDescriptor protoreflect.FieldDescriptor,
	transform payloadTransform,
) error {
	if valueDescriptor.Message() == nil {
		return nil
	}
	keys := make([]protoreflect.MapKey, 0, values.Len())
	values.Range(func(key protoreflect.MapKey, _ protoreflect.Value) bool {
		keys = append(keys, key)
		return true
	})
	for _, key := range keys {
		value := values.Get(key)
		if valueDescriptor.Message().FullName() == payloadMessageName {
			converted, err := transformSinglePayload(payloadFromMessage(value.Message()), transform)
			if err != nil {
				return err
			}
			values.Set(key, protoreflect.ValueOfMessage(converted.ProtoReflect()))
			continue
		}
		if err := transformMessagePayloads(value.Message(), transform); err != nil {
			return err
		}
	}
	return nil
}

func transformPayloadList(list protoreflect.List, transform payloadTransform) error {
	payloads := make([]*commonpb.Payload, list.Len())
	for i := 0; i < list.Len(); i++ {
		payloads[i] = payloadFromMessage(list.Get(i).Message())
	}
	converted, err := transform(payloads)
	if err != nil {
		return err
	}
	for _, payload := range converted {
		if payload == nil {
			return errors.New("payload codec returned a nil payload")
		}
	}
	list.Truncate(0)
	for _, payload := range converted {
		list.Append(protoreflect.ValueOfMessage(payload.ProtoReflect()))
	}
	return nil
}

func transformSinglePayload(payload *commonpb.Payload, transform payloadTransform) (*commonpb.Payload, error) {
	converted, err := transform([]*commonpb.Payload{payload})
	if err != nil {
		return nil, err
	}
	if len(converted) != 1 || converted[0] == nil {
		return nil, fmt.Errorf("payload codec returned %d payloads for a singular field", len(converted))
	}
	return converted[0], nil
}

func payloadFromMessage(message protoreflect.Message) *commonpb.Payload {
	return message.Interface().(*commonpb.Payload)
}
