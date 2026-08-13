package converter

var defaultPayloadConverter = NewCompositePayloadConverter(
	NewNilPayloadConverter(),
	NewByteSlicePayloadConverter(),
	NewProtoJSONPayloadConverter(),
	NewProtoPayloadConverter(),
	NewJSONPayloadConverter(),
)

// GetDefaultPayloadConverter returns the converter used by workers unless one is configured.
func GetDefaultPayloadConverter() PayloadConverter {
	return defaultPayloadConverter
}
