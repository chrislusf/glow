package flow

import (
	"log"
	"reflect"
)

func DefaultStringComparator(a, b string) int64 {
	switch {
	case a == b:
		return 0
	case a < b:
		return -1
	default:
		return 1
	}
}
func DefaultFloat64Comparator(a, b float64) int64 {
	switch {
	case a == b:
		return 0
	case a < b:
		return -1
	default:
		return 1
	}
}
func DefaultFloat32Comparator(a, b float32) int64 {
	switch {
	case a == b:
		return 0
	case a < b:
		return -1
	default:
		return 1
	}
}

func getComparator(dt reflect.Type) (funcPointer interface{}) {
	switch dt.Kind() {
	case reflect.Int:
		funcPointer = func(a, b int) int64 { return int64(a - b) }
	case reflect.Int8:
		funcPointer = func(a, b int8) int64 { return int64(a - b) }
	case reflect.Int16:
		funcPointer = func(a, b int16) int64 { return int64(a - b) }
	case reflect.Int32:
		funcPointer = func(a, b int32) int64 { return int64(a - b) }
	case reflect.Uint:
		funcPointer = func(a, b uint) int64 { return int64(a - b) }
	case reflect.Uint8:
		funcPointer = func(a, b uint8) int64 { return int64(a - b) }
	case reflect.Uint16:
		funcPointer = func(a, b uint16) int64 { return int64(a - b) }
	case reflect.Uint32:
		funcPointer = func(a, b uint32) int64 { return int64(a - b) }
	case reflect.Uint64:
		funcPointer = func(a, b uint64) int64 { return int64(a - b) }
	case reflect.Int64:
		funcPointer = func(a, b int64) int64 { return a - b }
	case reflect.Float32:
		funcPointer = DefaultFloat32Comparator
	case reflect.Float64:
		funcPointer = DefaultFloat64Comparator
	case reflect.String:
		funcPointer = DefaultStringComparator
	default:
		log.Panicf("No default comparator for %s:%s", dt.String(), dt.Kind().String())
	}
	return
}

func getSameKeyValues(ch chan reflect.Value,
	comparator func(a, b interface{}) int64,
	theKey, firstValue interface{}, hasFirstValue bool) (
	nextKey, nextValue interface{}, theValues []interface{}, hasValue bool) {

	theValues = append(theValues, firstValue)
	hasValue = hasFirstValue
	for {
		nextKey, nextValue, hasValue = getKeyValue(ch)
		if hasValue && comparator(theKey, nextKey) == 0 {
			theValues = append(theValues, nextValue)
		} else {
			return
		}
	}
	return
}

func getKeyValue(ch chan reflect.Value) (key, value interface{}, ok bool) {
	keyValue, hasValue := <-ch
	if hasValue {
		kv := keyValue.Interface().(KeyValue)
		key = kv.Key
		value = kv.Value
	}
	return key, value, hasValue
}
