package flow

import (
	"reflect"
)

// map can work with multiple kinds of inputs and outputs
// Input Types:
//   1. single value
//   2. (key, value) : Most common format for key value pair
//   3. (key, values) : GroupByKey() outputs
//   4. (key, values1, values2) : CoGroup() outputs
//   5. (key, value1, value2) : Join() outputs
// Output Types:
//   1. return single value
//   2. return (key, value)
//   3. return no value
//   4. return no value, but last parameter is a output channel
func (d *Dataset) Map(f interface{}) *Dataset {
	outType := guessFunctionOutputType(f)
	ret, step := add1ShardTo1Step(d, outType)
	step.Name = "Map"
	step.Function = func(task *Task) {

		invokeMapFunc := _buildMapperFunction(f, task)

		for input := range task.InputChan() {
			invokeMapFunc(input)
		}
		// println("exiting d:", d.Id, "step:", step.Id, "task:", task.Id)
	}
	return ret
}

func _buildMapperFunction(f interface{}, task *Task) func(input reflect.Value) {
	fn, ft := reflect.ValueOf(f), reflect.TypeOf(f)

	var outChan reflect.Value
	if ft.In(ft.NumIn()-1).Kind() == reflect.Chan || ft.NumOut() > 0 {
		outChan = task.Outputs[0].WriteChan
	}

	if ft.In(ft.NumIn()-1).Kind() == reflect.Chan {
		return _buildMapperFunctionWithChannel(fn, outChan)
	}
	return _buildMapperFunctionWithoutChannel(fn, outChan)
}

// if last parameter in the function is a channel
// use the channel element type as output type
func _buildMapperFunctionWithChannel(fn, outChan reflect.Value) func(input reflect.Value) {
	return func(input reflect.Value) {
		switch input.Type() {
		case KeyValueType:
			kv := input.Interface().(KeyValue)
			_functionCallWithChanOutput(fn, outChan, kv.Key, kv.Value)
		case KeyValueValueType:
			kv := input.Interface().(KeyValueValue)
			_functionCallWithChanOutput(fn, outChan, kv.Key, kv.Value1, kv.Value2)
		case KeyValuesType:
			kvs := input.Interface().(KeyValues)
			_functionCallWithChanOutput(fn, outChan, kvs.Key, kvs.Values)
		case KeyValuesValuesType:
			kvv := input.Interface().(KeyValuesValues)
			_functionCallWithChanOutput(fn, outChan, kvv.Key, kvv.Values1, kvv.Values2)
		default:
			fn.Call([]reflect.Value{input, outChan})
		}
	}
}

func _buildMapperFunctionWithoutChannel(fn, outChan reflect.Value) func(input reflect.Value) {
	return func(input reflect.Value) {
		outs := _functionCallBasedOnInputType(fn, input)
		sendMapOutputs(outChan, outs)
	}
}

func _functionCallWithChanOutput(fn reflect.Value, outChan reflect.Value, inputs ...interface{}) []reflect.Value {
	var args []reflect.Value
	for _, input := range inputs {
		args = append(args, reflect.ValueOf(input))
	}
	args = append(args, outChan)
	return fn.Call(args)
}

func _functionCall(fn reflect.Value, inputs ...interface{}) []reflect.Value {
	var args []reflect.Value
	for _, input := range inputs {
		args = append(args, reflect.ValueOf(input))
	}
	return fn.Call(args)
}

func _functionCallBasedOnInputType(fn, input reflect.Value) (outs []reflect.Value) {
	switch input.Type() {
	case KeyValueType:
		kv := input.Interface().(KeyValue)
		outs = _functionCall(fn, kv.Key, kv.Value)
	case KeyValueValueType:
		kv := input.Interface().(KeyValueValue)
		outs = _functionCall(fn, kv.Key, kv.Value1, kv.Value2)
	case KeyValuesType:
		kvs := input.Interface().(KeyValues)
		outs = _functionCall(fn, kvs.Key, kvs.Values)
	case KeyValuesValuesType:
		kvv := input.Interface().(KeyValuesValues)
		outs = _functionCall(fn, kvv.Key, kvv.Values1, kvv.Values2)
	default:
		outs = fn.Call([]reflect.Value{input})
	}
	return
}

// f(A)bool
func (d *Dataset) Filter(f interface{}) *Dataset {
	ret, step := add1ShardTo1Step(d, d.Type)
	ret.IsKeyPartitioned = d.IsKeyPartitioned
	ret.IsKeyLocalSorted = d.IsKeyLocalSorted
	step.Name = "Filter"
	step.Function = func(task *Task) {
		fn := reflect.ValueOf(f)
		outChan := task.Outputs[0].WriteChan
		for input := range task.InputChan() {
			outs := _functionCallBasedOnInputType(fn, input)
			if len(outs) > 0 && outs[0].Bool() {
				outChan.Send(input)
			}
		}
	}
	return ret
}

func add1ShardTo1Step(d *Dataset, nextDataType reflect.Type) (ret *Dataset, step *Step) {
	ret = d.context.newNextDataset(len(d.Shards), nextDataType)
	step = d.context.AddOneToOneStep(d, ret)
	return
}

// the value over the outChan is always reflect.Value
// but the inner values are always actual interface{} object
func sendMapOutputs(outChan reflect.Value, values []reflect.Value) {
	if !outChan.IsValid() {
		return
	}
	if len(values) == 2 {
		outChan.Send(reflect.ValueOf(KeyValue{values[0].Interface(), values[1].Interface()}))
		return
	}
	if len(values) == 1 {
		outChan.Send(values[0])
		return
	}
}
