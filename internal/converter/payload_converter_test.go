package converter

import (
	"bytes"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
)

func TestDefaultPayloadConverterRoundTripsSupportedValues(t *testing.T) {
	converter := GetDefaultPayloadConverter()
	protoValue := &commonpb.WorkflowType{Name: "workflow"}

	tests := []struct {
		name     string
		value    any
		encoding string
		result   any
		check    func(t *testing.T, result any)
	}{
		{
			name: "nil", value: nil, encoding: MetadataEncodingNil, result: new(*string),
			check: func(t *testing.T, result any) {
				if got := *result.(**string); got != nil {
					t.Fatalf("decoded nil is %v", got)
				}
			},
		},
		{
			name: "bytes", value: []byte("data"), encoding: MetadataEncodingBinary, result: new([]byte),
			check: func(t *testing.T, result any) {
				if got := *result.(*[]byte); !bytes.Equal(got, []byte("data")) {
					t.Fatalf("decoded bytes are %q", got)
				}
			},
		},
		{
			name: "protobuf", value: protoValue, encoding: MetadataEncodingProtoJSON, result: new(*commonpb.WorkflowType),
			check: func(t *testing.T, result any) {
				if got := (*result.(**commonpb.WorkflowType)).GetName(); got != "workflow" {
					t.Fatalf("decoded proto name is %q", got)
				}
			},
		},
		{
			name: "json", value: map[string]int{"count": 3}, encoding: MetadataEncodingJSON, result: new(map[string]int),
			check: func(t *testing.T, result any) {
				if got := (*result.(*map[string]int))["count"]; got != 3 {
					t.Fatalf("decoded count is %d", got)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, err := converter.ToPayload(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(payload.Metadata[MetadataEncoding]); got != test.encoding {
				t.Fatalf("encoding is %q, want %q", got, test.encoding)
			}
			if err := converter.FromPayload(payload, test.result); err != nil {
				t.Fatal(err)
			}
			test.check(t, test.result)
		})
	}
}

func TestRawValuePassesPayloadThrough(t *testing.T) {
	want := &commonpb.Payload{
		Metadata: map[string][]byte{MetadataEncoding: []byte("custom/encoding")},
		Data:     []byte("opaque"),
	}
	converter := GetDefaultPayloadConverter()
	payload, err := converter.ToPayload(NewRawValue(want))
	if err != nil {
		t.Fatal(err)
	}
	if payload != want {
		t.Fatal("raw payload was copied or converted")
	}
	var decoded RawValue
	if err := converter.FromPayload(want, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Payload() != want {
		t.Fatal("decoded raw payload was copied or converted")
	}
}
