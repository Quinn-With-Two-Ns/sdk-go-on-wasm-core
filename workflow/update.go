package workflow

import internalworkflow "github.com/temporalio/sdk-go-on-wasm-core/internal/workflowworker"

// UpdateInfo identifies the update executing in a workflow context.
type UpdateInfo = internalworkflow.UpdateInfo

// UpdateHandlerOptions configures a workflow update handler.
type UpdateHandlerOptions struct {
	// Validator may accept the same serializable arguments as the handler, with an optional
	// *workflow.Context first, and must return error. Validators must not mutate workflow state.
	Validator any
}

// SetUpdateHandler registers handler for updateName on the current workflow execution.
// The handler must accept *workflow.Context first and return error or (result, error).
func SetUpdateHandler(ctx *Context, updateName string, handler any) error {
	return SetUpdateHandlerWithOptions(ctx, updateName, handler, UpdateHandlerOptions{})
}

// SetUpdateHandlerWithOptions registers a workflow update handler and optional validator.
func SetUpdateHandlerWithOptions(
	ctx *Context,
	updateName string,
	handler any,
	options UpdateHandlerOptions,
) error {
	return internalworkflow.SetUpdateHandler(ctx, updateName, handler, internalworkflow.UpdateHandlerOptions{
		Validator: options.Validator,
	})
}

// GetCurrentUpdateInfo returns information about the update running in ctx, or nil outside an
// update handler or validator.
func GetCurrentUpdateInfo(ctx *Context) *UpdateInfo {
	return internalworkflow.GetCurrentUpdateInfo(ctx)
}
