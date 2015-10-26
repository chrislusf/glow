package flow

import (
	"reflect"
)

func guessFunctionOutputType(f interface{}) reflect.Type {
	ft := reflect.TypeOf(f)
	if ft.In(ft.NumIn()-1).Kind() == reflect.Chan {
		return ft.In(ft.NumIn() - 1).Elem()
	}
	if ft.NumOut() == 1 {
		return ft.Out(0)
	}
	if ft.NumOut() == 2 {
		return KeyValueType
	}
	return nil
}

func guessKey(input reflect.Value) (key reflect.Value) {
	switch input.Kind() {
	case reflect.Slice:
		key = input.Index(0)
	case reflect.Struct:
		key = input.Field(0)
	case reflect.Array:
		key = input.Index(0)
	default:
		key = input
	}
	if v, ok := key.Interface().(reflect.Value); ok {
		return v
	}
	return key
}
