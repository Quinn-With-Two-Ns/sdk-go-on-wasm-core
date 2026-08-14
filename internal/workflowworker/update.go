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
	"google.golang.org/protobuf/types/known/emptypb"
)

var updateErrorType = reflect.TypeFor[error]()

// UpdateHandlerOptions configures a workflow update handler.
type UpdateHandlerOptions struct {
	Validator any
}

// UpdateInfo identifies the update executing in the current workflow context.
type UpdateInfo struct {
	ID   string
	Name string
}

type registeredUpdate struct {
	name      string
	handler   *invoker.Function
	validator *invoker.Function
}

// SetUpdateHandler registers handler for updateName on the current workflow execution.
func SetUpdateHandler(ctx *Context, updateName string, handler any, options UpdateHandlerOptions) error {
	ctx.requireRunning("SetUpdateHandler")
	update, err := newRegisteredUpdate(updateName, handler, options.Validator)
	if err != nil {
		return err
	}
	ctx.execution.updateHandlers[updateName] = update
	return nil
}

// GetCurrentUpdateInfo returns information about the update running in ctx, or nil otherwise.
func GetCurrentUpdateInfo(ctx *Context) *UpdateInfo {
	if ctx == nil || ctx.updateInfo == nil {
		return nil
	}
	info := *ctx.updateInfo
	return &info
}

func newRegisteredUpdate(name string, handler, validator any) (*registeredUpdate, error) {
	if strings.HasPrefix(name, "__") {
		return nil, errors.New("update names beginning with \"__\" are reserved")
	}
	handlerType, err := updateHandlerType(name, handler)
	if err != nil {
		return nil, err
	}
	registeredHandler, err := invoker.New(
		"update", name, handler, reflect.TypeFor[*Context](), true, "*workflow.Context",
	)
	if err != nil {
		return nil, err
	}
	update := &registeredUpdate{name: name, handler: registeredHandler}
	if validator == nil {
		return update, nil
	}
	validatorType, err := updateValidatorType(name, validator)
	if err != nil {
		return nil, err
	}
	if err := validateUpdateParameters(handlerType, validatorType); err != nil {
		return nil, fmt.Errorf("update %q validator: %w", name, err)
	}
	update.validator, err = invoker.New(
		"update validator", name, validator, reflect.TypeFor[*Context](), false, "",
	)
	if err != nil {
		return nil, err
	}
	return update, nil
}

func updateHandlerType(name string, handler any) (reflect.Type, error) {
	if handler == nil {
		return nil, fmt.Errorf("update %q function is required", name)
	}
	handlerType := reflect.TypeOf(handler)
	if handlerType.Kind() != reflect.Func {
		return nil, fmt.Errorf("update %q must be a function, got %T", name, handler)
	}
	if handlerType.NumIn() == 0 || handlerType.In(0) != reflect.TypeFor[*Context]() {
		return nil, fmt.Errorf("update %q must accept *workflow.Context first", name)
	}
	return handlerType, nil
}

func updateValidatorType(name string, validator any) (reflect.Type, error) {
	validatorType := reflect.TypeOf(validator)
	if validatorType.Kind() != reflect.Func {
		return nil, fmt.Errorf("update %q validator must be a function, got %T", name, validator)
	}
	if validatorType.NumOut() != 1 || validatorType.Out(0) != updateErrorType {
		return nil, fmt.Errorf("update %q validator must return error", name)
	}
	return validatorType, nil
}

func validateUpdateParameters(handlerType, validatorType reflect.Type) error {
	handlerParameters := parameterTypesWithoutContext(handlerType)
	validatorParameters := parameterTypesWithoutContext(validatorType)
	if len(handlerParameters) != len(validatorParameters) {
		return errors.New("parameters must match the update handler")
	}
	for i := range handlerParameters {
		if handlerParameters[i] != validatorParameters[i] {
			return fmt.Errorf("parameter %d is %v, want %v", i, validatorParameters[i], handlerParameters[i])
		}
	}
	return nil
}

func parameterTypesWithoutContext(functionType reflect.Type) []reflect.Type {
	start := 0
	if functionType.NumIn() > 0 && functionType.In(0) == reflect.TypeFor[*Context]() {
		start = 1
	}
	parameters := make([]reflect.Type, 0, functionType.NumIn()-start)
	for i := start; i < functionType.NumIn(); i++ {
		parameters = append(parameters, functionType.In(i))
	}
	return parameters
}

func (u *registeredUpdate) validate(
	ctx *Context,
	payloadConverter converter.PayloadConverter,
	input *invoker.DecodedInput,
) error {
	if u.validator == nil {
		return nil
	}
	_, err := u.validator.ExecuteDecoded(ctx, payloadConverter, input)
	return err
}

func (u *registeredUpdate) execute(
	ctx *Context,
	payloadConverter converter.PayloadConverter,
	input *invoker.DecodedInput,
) (*commonpb.Payload, error) {
	return u.handler.ExecuteDecoded(ctx, payloadConverter, input)
}

func (e *workflowExecution) startUpdate(update *workflowactivation.DoUpdate) {
	handler := e.updateHandlers[update.Name]
	if handler == nil {
		e.emit(updateRejected(update.ProtocolInstanceId, fmt.Errorf("update %q is not registered", update.Name)))
		return
	}
	input, err := handler.handler.DecodeInput(e.payloadConverter, update.Input)
	if err != nil {
		e.emit(updateRejected(update.ProtocolInstanceId, err))
		return
	}
	info := &UpdateInfo{ID: update.Id, Name: update.Name}
	if update.RunValidator && handler.validator != nil {
		validationContext := &Context{execution: e, updateInfo: info}
		if err := e.validateUpdateSafely(handler, validationContext, input); err != nil {
			e.emit(updateRejected(update.ProtocolInstanceId, err))
			return
		}
	}
	e.emit(updateAccepted(update.ProtocolInstanceId))
	e.addCoroutineWithUpdateInfo("update:"+update.Name+":"+update.Id, info, func(ctx *Context) {
		result, err := handler.execute(ctx, e.payloadConverter, input)
		if err != nil {
			e.emit(updateRejected(update.ProtocolInstanceId, err))
			return
		}
		e.emit(updateCompleted(update.ProtocolInstanceId, result))
	})
}

func (e *workflowExecution) validateUpdateSafely(
	handler *registeredUpdate,
	ctx *Context,
	input *invoker.DecodedInput,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("update %q validator panicked: %v\n%s", handler.name, recovered, debug.Stack())
		}
	}()
	if err := handler.validate(ctx, e.payloadConverter, input); err != nil {
		return fmt.Errorf("validate update %q: %w", handler.name, err)
	}
	return nil
}

func updateAccepted(protocolInstanceID string) *workflowcommands.WorkflowCommand {
	return &workflowcommands.WorkflowCommand{
		Variant: &workflowcommands.WorkflowCommand_UpdateResponse{
			UpdateResponse: &workflowcommands.UpdateResponse{
				ProtocolInstanceId: protocolInstanceID,
				Response: &workflowcommands.UpdateResponse_Accepted{
					Accepted: &emptypb.Empty{},
				},
			},
		},
	}
}

func updateRejected(protocolInstanceID string, err error) *workflowcommands.WorkflowCommand {
	return &workflowcommands.WorkflowCommand{
		Variant: &workflowcommands.WorkflowCommand_UpdateResponse{
			UpdateResponse: &workflowcommands.UpdateResponse{
				ProtocolInstanceId: protocolInstanceID,
				Response: &workflowcommands.UpdateResponse_Rejected{
					Rejected: newFailure(err, "GoWasmUpdateError", ""),
				},
			},
		},
	}
}

func updateCompleted(protocolInstanceID string, result *commonpb.Payload) *workflowcommands.WorkflowCommand {
	return &workflowcommands.WorkflowCommand{
		Variant: &workflowcommands.WorkflowCommand_UpdateResponse{
			UpdateResponse: &workflowcommands.UpdateResponse{
				ProtocolInstanceId: protocolInstanceID,
				Response: &workflowcommands.UpdateResponse_Completed{
					Completed: result,
				},
			},
		},
	}
}
