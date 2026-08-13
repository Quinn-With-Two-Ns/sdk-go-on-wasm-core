package converter

import "reflect"

func pointerTo(value any) reflect.Value {
	valuePtr := reflect.New(reflect.TypeOf(value))
	valuePtr.Elem().Set(reflect.ValueOf(value))
	return valuePtr
}

func newOfSameType(value reflect.Value) reflect.Value {
	newValue := reflect.New(value.Type().Elem())
	value.Set(newValue)
	return newValue
}

func isInterfaceNil(value any) bool {
	v := reflect.ValueOf(value)
	return value == nil || (v.Kind() == reflect.Ptr && v.IsNil())
}
