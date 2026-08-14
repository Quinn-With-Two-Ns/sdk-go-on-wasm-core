package workflowworker

import (
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
	workflowactivation "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowactivation"
	workflowcommands "github.com/temporalio/sdk-go-on-wasm-core/internal/corepb/workflowcommands"
	"github.com/temporalio/sdk-go-on-wasm-core/internal/invoker"
	commonpb "go.temporal.io/api/common/v1"
)

type registeredQuery struct {
	name     string
	function *invoker.Function
}

// SetQueryHandler registers a read-only handler for queries of queryType.
func SetQueryHandler(ctx *Context, queryType string, handler any) error {
	ctx.requireRunning("SetQueryHandler")
	query, err := newRegisteredQuery(queryType, handler)
	if err != nil {
		return err
	}
	ctx.execution.queryHandlers[queryType] = query
	return nil
}

func newRegisteredQuery(name string, handler any) (*registeredQuery, error) {
	if strings.HasPrefix(name, "__") {
		return nil, errors.New("query type names beginning with \"__\" are reserved")
	}
	if handler != nil {
		handlerType := reflect.TypeOf(handler)
		if handlerType.Kind() == reflect.Func {
			if handlerType.NumIn() > 0 && handlerType.In(0) == reflect.TypeFor[*Context]() {
				return nil, fmt.Errorf("query %q must not accept *workflow.Context", name)
			}
			if handlerType.NumOut() != 2 {
				return nil, fmt.Errorf("query %q must return (result, error)", name)
			}
		}
	}
	function, err := invoker.New("query", name, handler, nil, false, "")
	if err != nil {
		return nil, err
	}
	return &registeredQuery{name: name, function: function}, nil
}

func (q *registeredQuery) execute(
	payloadConverter converter.PayloadConverter,
	input []*commonpb.Payload,
) (*commonpb.Payload, error) {
	return q.function.Execute(nil, payloadConverter, input)
}

func (e *workflowExecution) respondToQuery(query *workflowactivation.QueryWorkflow) *workflowcommands.WorkflowCommand {
	result := &workflowcommands.QueryResult{QueryId: query.QueryId}
	response, err := e.executeQuerySafely(query)
	if err != nil {
		result.Variant = &workflowcommands.QueryResult_Failed{
			Failed: newFailure(err, "GoWasmQueryError", ""),
		}
	} else {
		result.Variant = &workflowcommands.QueryResult_Succeeded{
			Succeeded: &workflowcommands.QuerySuccess{Response: response},
		}
	}
	return &workflowcommands.WorkflowCommand{
		Variant: &workflowcommands.WorkflowCommand_RespondToQuery{RespondToQuery: result},
	}
}

func (e *workflowExecution) executeQuerySafely(
	query *workflowactivation.QueryWorkflow,
) (response *commonpb.Payload, err error) {
	handler := e.queryHandlers[query.QueryType]
	if handler == nil {
		return nil, fmt.Errorf("query %q is not registered", query.QueryType)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("query %q panicked: %v\n%s", query.QueryType, recovered, debug.Stack())
		}
	}()
	return handler.execute(e.payloadConverter, query.Arguments)
}
