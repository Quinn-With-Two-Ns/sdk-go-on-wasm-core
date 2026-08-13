// Package functionname derives Temporal operation names from Go functions.
package functionname

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// Get returns the short runtime name of function, including method support.
func Get(kind string, function any) (string, error) {
	if function == nil {
		return "", fmt.Errorf("%s function is required", kind)
	}
	value := reflect.ValueOf(function)
	if value.Kind() != reflect.Func || value.IsNil() {
		return "", fmt.Errorf("%s must be a function, got %T", kind, function)
	}
	runtimeFunction := runtime.FuncForPC(value.Pointer())
	if runtimeFunction == nil {
		return "", fmt.Errorf("cannot determine %s function name", kind)
	}
	parts := strings.Split(runtimeFunction.Name(), ".")
	return strings.TrimSuffix(parts[len(parts)-1], "-fm"), nil
}
