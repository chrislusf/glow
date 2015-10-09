package flow

import (
	"log"
	"reflect"
)

func _getLessThanComparatorByKeyValue(key reflect.Value) (funcPointer interface{}) {
	dt := key.Type()
	if key.Kind() == reflect.Interface {
		dt = reflect.TypeOf(key.Interface())
	}
	switch dt.Kind() {
	case reflect.Int:
		funcPointer = func(a, b int) bool { return a < b }
	case reflect.Int8:
		funcPointer = func(a, b int8) bool { return a < b }
	case reflect.Int16:
		funcPointer = func(a, b int16) bool { return a < b }
	case reflect.Int32:
		funcPointer = func(a, b int32) bool { return a < b }
	case reflect.Int64:
		funcPointer = func(a, b int64) bool { return a < b }
	case reflect.Uint:
		funcPointer = func(a, b uint) bool { return a < b }
	case reflect.Uint8:
		funcPointer = func(a, b uint8) bool { return a < b }
	case reflect.Uint16:
		funcPointer = func(a, b uint16) bool { return a < b }
	case reflect.Uint32:
		funcPointer = func(a, b uint32) bool { return a < b }
	case reflect.Uint64:
		funcPointer = func(a, b uint64) bool { return a < b }
	case reflect.Float32:
		funcPointer = func(a, b float32) bool { return a < b }
	case reflect.Float64:
		funcPointer = func(a, b float64) bool { return a < b }
	case reflect.String:
		funcPointer = func(a, b string) bool { return a < b }
	default:
		log.Panicf("No default less than comparator for type:%s, kind:%s", dt.String(), dt.Kind().String())
	}
	return
}

func getLessThanComparator(datasetType reflect.Type, key reflect.Value, functionPointer interface{}) func(a interface{}, b interface{}) bool {
	lessThanFuncValue := reflect.ValueOf(functionPointer)
	if functionPointer == nil {
		v := guessKey(key)
		lessThanFuncValue = reflect.ValueOf(_getLessThanComparatorByKeyValue(v))
	}
	if datasetType.Kind() == reflect.Slice {
		return func(a interface{}, b interface{}) bool {
			// println("a:", reflect.ValueOf(a).Field(0).Kind().String(), "lessThanFuncValue:", lessThanFuncValue.String())
			ret := lessThanFuncValue.Call([]reflect.Value{
				reflect.ValueOf(reflect.ValueOf(a).Index(0).Interface()),
				reflect.ValueOf(reflect.ValueOf(b).Index(0).Interface()),
			})
			return ret[0].Bool()
		}
	} else {
		return func(a interface{}, b interface{}) bool {
			ret := lessThanFuncValue.Call([]reflect.Value{
				reflect.ValueOf(a),
				reflect.ValueOf(b),
			})
			return ret[0].Bool()
		}
	}
}
