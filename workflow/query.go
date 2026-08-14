package workflow

import internalworkflow "github.com/temporalio/sdk-go-on-wasm-core/internal/workflowworker"

// SetQueryHandler registers handler for queryType on the current workflow execution.
// The handler may accept serializable arguments and must return (result, error).
// Query handlers run outside the workflow coroutine and must not call command-producing or
// blocking workflow APIs or mutate workflow state.
func SetQueryHandler(ctx *Context, queryType string, handler any) error {
	return internalworkflow.SetQueryHandler(ctx, queryType, handler)
}
