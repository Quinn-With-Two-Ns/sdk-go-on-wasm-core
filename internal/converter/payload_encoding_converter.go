package converter

import commonpb "go.temporal.io/api/common/v1"

// PayloadEncodingConverter handles one Temporal payload encoding.
type PayloadEncodingConverter interface {
	ToPayload(value any) (*commonpb.Payload, error)
	FromPayload(payload *commonpb.Payload, valuePtr any) error
	ToString(payload *commonpb.Payload) string
	Encoding() string
}

type protoPayloadEncodingConverter interface {
	PayloadEncodingConverter
	ExcludeProtobufMessageTypes() bool
}

func newPayload(data []byte, c PayloadEncodingConverter) *commonpb.Payload {
	return &commonpb.Payload{
		Metadata: map[string][]byte{MetadataEncoding: []byte(c.Encoding())},
		Data:     data,
	}
}

func newProtoPayload(data []byte, c protoPayloadEncodingConverter, messageType string) *commonpb.Payload {
	if c.ExcludeProtobufMessageTypes() {
		return newPayload(data, c)
	}
	return &commonpb.Payload{
		Metadata: map[string][]byte{
			MetadataEncoding:    []byte(c.Encoding()),
			MetadataMessageType: []byte(messageType),
		},
		Data: data,
	}
}
