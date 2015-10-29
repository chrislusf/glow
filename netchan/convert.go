package netchan

import (
	// "fmt"
	"reflect"
)

func CleanObject(v reflect.Value, currentType, expectedType reflect.Type) reflect.Value {

	if currentType == expectedType {
		// fmt.Printf("sending1 %v to output chan\n", t)
		return v
	} else {
		// convert []interface{} to a struct
		// fmt.Printf("sending %v of type %s as type %s to output chan\n", v, currentType.String(), expectedType.String())
		x := reflect.New(expectedType).Elem()
		for i := 0; i < expectedType.NumField(); i++ {
			x.Field(i).Set(reflect.Indirect(v.Field(i).Elem()))
		}
		return x
	}
}
