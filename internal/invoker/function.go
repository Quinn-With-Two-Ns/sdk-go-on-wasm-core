// Package invoker validates and calls registered workflow and activity functions.
package invoker

import (
	"fmt"
	"reflect"

	"github.com/temporalio/sdk-go-on-wasm-core/internal/converter"
	commonpb "go.temporal.io/api/common/v1"
)

var errorType = reflect.TypeFor[error]()

// Function is a validated workflow or activity function.
type Function struct {
	kind       string
	name       string
	function   reflect.Value
	inputStart int
	result     bool
}

// New validates a registered function. The context parameter may be required
// for workflows or optional for activities.
func New(
	kind string,
	name string,
	function any,
	contextType reflect.Type,
	contextRequired bool,
	contextDescription string,
) (*Function, error) {
	if name == "" {
		return nil, fmt.Errorf("%s name is required", kind)
	}
	if function == nil {
		return nil, fmt.Errorf("%s function is required", kind)
	}

	functionValue := reflect.ValueOf(function)
	functionType := functionValue.Type()
	if functionType.Kind() != reflect.Func || functionValue.IsNil() {
		return nil, fmt.Errorf("%s %q must be a function, got %T", kind, name, function)
	}

	inputStart := 0
	if functionType.NumIn() > 0 && functionType.In(0) == contextType {
		inputStart = 1
	} else if contextRequired {
		return nil, fmt.Errorf("%s %q must accept %s first", kind, name, contextDescription)
	}
	if functionType.NumOut() < 1 || functionType.NumOut() > 2 || functionType.Out(functionType.NumOut()-1) != errorType {
		return nil, fmt.Errorf("%s %q must return error or (result, error)", kind, name)
	}

	return &Function{
		kind:       kind,
		name:       name,
		function:   functionValue,
		inputStart: inputStart,
		result:     functionType.NumOut() == 2,
	}, nil
}

// Execute decodes payload arguments, invokes the function, and encodes its result.
func (f *Function) Execute(
	contextArgument any,
	payloadConverter converter.PayloadConverter,
	input []*commonpb.Payload,
) (*commonpb.Payload, error) {
	functionType := f.function.Type()
	arguments := make([]reflect.Value, 0, functionType.NumIn())
	if f.inputStart == 1 {
		arguments = append(arguments, reflect.ValueOf(contextArgument))
	}

	valuePointers := make([]any, 0, functionType.NumIn()-f.inputStart)
	for i := f.inputStart; i < functionType.NumIn(); i++ {
		valuePointer := reflect.New(functionType.In(i))
		valuePointers = append(valuePointers, valuePointer.Interface())
		arguments = append(arguments, valuePointer.Elem())
	}
	if err := payloadConverter.FromPayloads(&commonpb.Payloads{Payloads: input}, valuePointers...); err != nil {
		return nil, fmt.Errorf("decode %s %q input: %w", f.kind, f.name, err)
	}

	results := f.function.Call(arguments)
	if errValue := results[len(results)-1]; !errValue.IsNil() {
		return nil, errValue.Interface().(error)
	}
	if !f.result {
		return nil, nil
	}
	payload, err := payloadConverter.ToPayload(results[0].Interface())
	if err != nil {
		return nil, fmt.Errorf("encode %s %q result: %w", f.kind, f.name, err)
	}
	return payload, nil
}
