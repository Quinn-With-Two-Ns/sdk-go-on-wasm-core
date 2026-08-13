// Package converter serializes values to and from Temporal payloads for the
// internal worker implementations.
//
// This package is derived from go.temporal.io/sdk/converter at sdk-go commit
// 10575057a51940a49bd23d79af309f5975bb7d2e. Legacy gogo-protobuf support and
// converter features not used by this worker are intentionally omitted.
package converter

import commonpb "go.temporal.io/api/common/v1"

// PayloadConverter converts Go values to and from unencoded Temporal payloads.
// Payload codecs run separately when Core messages enter or leave a worker.
type PayloadConverter interface {
	ToPayload(value any) (*commonpb.Payload, error)
	FromPayload(payload *commonpb.Payload, valuePtr any) error
	ToPayloads(values ...any) (*commonpb.Payloads, error)
	FromPayloads(payloads *commonpb.Payloads, valuePtrs ...any) error
}
