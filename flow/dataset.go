package flow

import (
	"reflect"
)

func (d *Dataset) GetShards() []*DatasetShard {
	return d.Shards
}

type Dataset struct {
	Id                  int
	context             *FlowContext
	Type                reflect.Type
	Shards              []*DatasetShard
	Step                *Step
	ReadingSteps        []*Step
	ExternalInputChans  []reflect.Value
	ExternalOutputChans []reflect.Value
}

func NewDataset(context *FlowContext, t reflect.Type) *Dataset {
	d := &Dataset{
		Id:      len(context.Datasets),
		context: context,
		Type:    t,
	}
	context.Datasets = append(context.Datasets, d)
	return d
}

// key value can not use reflect.Value which can not be serailize/deserialze
type KeyValue struct {
	Key   interface{}
	Value interface{}
}

type KeyValueValue struct {
	Key    interface{}
	Value1 interface{}
	Value2 interface{}
}

type KeyValues struct {
	Key    interface{}
	Values interface{}
}

type KeyValuesValues struct {
	Key     interface{}
	Values1 interface{}
	Values2 interface{}
}

var (
	KeyValueType        = reflect.TypeOf(KeyValue{})
	KeyValueValueType   = reflect.TypeOf(KeyValueValue{})
	KeyValuesType       = reflect.TypeOf(KeyValues{})
	KeyValuesValuesType = reflect.TypeOf(KeyValuesValues{})
)
