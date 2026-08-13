// Package activity contains APIs used by activity implementations.
package activity

import (
	"context"

	internalactivity "github.com/temporalio/sdk-go-on-wasm-core/internal/activityworker"
)

// RecordHeartbeat records progress details for the currently executing activity.
func RecordHeartbeat(ctx context.Context, details ...any) error {
	return internalactivity.RecordHeartbeat(ctx, details...)
}
