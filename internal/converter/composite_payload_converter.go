package converter

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
)

// CompositePayloadConverter applies encoding converters in order when encoding and
// selects one by payload metadata when decoding.
type CompositePayloadConverter struct {
	encodingConverters map[string]PayloadEncodingConverter
	orderedEncodings   []string
}

func NewCompositePayloadConverter(encodingConverters ...PayloadEncodingConverter) PayloadConverter {
	c := &CompositePayloadConverter{
		encodingConverters: make(map[string]PayloadEncodingConverter, len(encodingConverters)),
		orderedEncodings:   make([]string, len(encodingConverters)),
	}
	for i, encodingConverter := range encodingConverters {
		c.encodingConverters[encodingConverter.Encoding()] = encodingConverter
		c.orderedEncodings[i] = encodingConverter.Encoding()
	}
	return c
}

func (c *CompositePayloadConverter) ToPayloads(values ...any) (*commonpb.Payloads, error) {
	if len(values) == 0 {
		return nil, nil
	}
	result := &commonpb.Payloads{}
	for i, value := range values {
		payload, err := c.ToPayload(value)
		if err != nil {
			return nil, fmt.Errorf("values[%d]: %w", i, err)
		}
		result.Payloads = append(result.Payloads, payload)
	}
	return result, nil
}

func (c *CompositePayloadConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...any) error {
	if payloads == nil {
		return nil
	}
	for i, payload := range payloads.GetPayloads() {
		if i >= len(valuePtrs) {
			break
		}
		if err := c.FromPayload(payload, valuePtrs[i]); err != nil {
			return fmt.Errorf("payload item %d: %w", i, err)
		}
	}
	return nil
}

func (c *CompositePayloadConverter) ToPayload(value any) (*commonpb.Payload, error) {
	if rawValue, ok := value.(RawValue); ok {
		return rawValue.Payload(), nil
	}
	for _, enc := range c.orderedEncodings {
		payload, err := c.encodingConverters[enc].ToPayload(value)
		if err != nil {
			return nil, err
		}
		if payload != nil {
			return payload, nil
		}
	}
	return nil, fmt.Errorf("value: %v of type: %T: %w", value, value, ErrUnableToFindConverter)
}

func (c *CompositePayloadConverter) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	if payload == nil {
		return nil
	}
	if rawValue, ok := valuePtr.(*RawValue); ok {
		*rawValue = NewRawValue(payload)
		return nil
	}
	enc, err := encoding(payload)
	if err != nil {
		return err
	}
	encodingConverter, ok := c.encodingConverters[enc]
	if !ok {
		return fmt.Errorf("encoding %s: %w", enc, ErrEncodingIsNotSupported)
	}
	return encodingConverter.FromPayload(payload, valuePtr)
}

func (c *CompositePayloadConverter) ToString(payload *commonpb.Payload) string {
	if payload == nil {
		return ""
	}
	enc, err := encoding(payload)
	if err != nil {
		return err.Error()
	}
	encodingConverter, ok := c.encodingConverters[enc]
	if !ok {
		return fmt.Errorf("encoding %s: %w", enc, ErrEncodingIsNotSupported).Error()
	}
	return encodingConverter.ToString(payload)
}

func (c *CompositePayloadConverter) ToStrings(payloads *commonpb.Payloads) []string {
	if payloads == nil {
		return nil
	}
	result := make([]string, 0, len(payloads.GetPayloads()))
	for _, payload := range payloads.GetPayloads() {
		result = append(result, c.ToString(payload))
	}
	return result
}

func encoding(payload *commonpb.Payload) (string, error) {
	metadata := payload.GetMetadata()
	if metadata == nil {
		return "", ErrMetadataIsNotSet
	}
	if enc, ok := metadata[MetadataEncoding]; ok {
		return string(enc), nil
	}
	return "", ErrEncodingIsNotSet
}
