package io

import (
	"reflect"
)

func CleanObject(v reflect.Value, currentType, expectedType reflect.Type) reflect.Value {

	if currentType == expectedType {
		// fmt.Printf("sending1 %v to output chan\n", t)
		return v
	} else {
		// convert []interface{} to a struct
		// fmt.Printf("sending %d %v to output chan\n", currentType.NumField(), t)
		x := reflect.New(expectedType).Elem()
		for i := 0; i < expectedType.NumField(); i++ {
			x.Field(i).Set(reflect.Indirect(v.Index(i).Elem()))
		}
		return x
	}
}
