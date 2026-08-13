// Package workerconfig normalizes configuration shared by workflow and activity workers.
package workerconfig

import (
	"crypto/tls"
	"fmt"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
)

const (
	defaultAddress   = "localhost:7233"
	defaultNamespace = "default"
	defaultIdentity  = "go-sdk-on-wasm-core"
)

// Connection contains transport and identity settings common to all Core workers.
type Connection struct {
	Address          string
	Namespace        string
	TaskQueue        string
	Identity         string
	PayloadConverter converter.PayloadConverter
	PayloadCodec     converter.PayloadCodec
	TLS              *tls.Config
	APIKey           string
}

// Normalize applies shared defaults and validates fields required by every worker.
func Normalize(kind string, connection Connection) (Connection, error) {
	if connection.Address == "" {
		connection.Address = defaultAddress
	}
	if connection.Namespace == "" {
		connection.Namespace = defaultNamespace
	}
	if connection.Identity == "" {
		connection.Identity = defaultIdentity
	}
	if connection.TaskQueue == "" {
		return Connection{}, fmt.Errorf("%s task queue is required", kind)
	}
	if connection.PayloadConverter == nil {
		connection.PayloadConverter = converter.GetDefaultPayloadConverter()
	}
	if connection.PayloadCodec == nil {
		connection.PayloadCodec = converter.NewNoopPayloadCodec()
	}
	return connection, nil
}
