package flow

import (
	"fmt"
	"reflect"
)

func (d *Dataset) AddOutput(ch interface{}) *Dataset {
	assertChannelOf(ch, d.Type)
	d.ExternalOutputChans = append(d.ExternalOutputChans, reflect.Indirect(reflect.ValueOf(ch)))
	return d
}

func assertChannelOf(ch interface{}, dsType reflect.Type) {
	chType := reflect.TypeOf(ch)
	if chType.Kind() != reflect.Chan {
		panic(fmt.Sprintf("%v should be a channel", ch))
	}
	if chType.Elem() == dsType {
		return
	}
	if chType.Elem().Kind() == reflect.Struct {
		return
	}
	panic(fmt.Sprintf("chan %s should have element type %s", chType, dsType))
}
