package converter

import (
	"encoding/json"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
)

type JSONPayloadConverter struct{}

func NewJSONPayloadConverter() *JSONPayloadConverter {
	return &JSONPayloadConverter{}
}

func (c *JSONPayloadConverter) ToPayload(value any) (*commonpb.Payload, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnableToEncode, err)
	}
	return newPayload(data, c), nil
}

func (*JSONPayloadConverter) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	if err := json.Unmarshal(payload.GetData(), valuePtr); err != nil {
		return fmt.Errorf("%w: %v", ErrUnableToDecode, err)
	}
	return nil
}

func (*JSONPayloadConverter) ToString(payload *commonpb.Payload) string {
	return string(payload.GetData())
}

func (*JSONPayloadConverter) Encoding() string { return MetadataEncodingJSON }
