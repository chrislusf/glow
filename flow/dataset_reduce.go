package flow

import (
	"reflect"
)

func (d *Dataset) Reduce(f interface{}) (ret *Dataset) {
	return d.LocalReduce(f).MergeReduce(f)
}

// f(V, V) V : less than function
// New Dataset contains V
func (d *Dataset) LocalReduce(f interface{}) *Dataset {
	ret, step := add1ShardTo1Step(d, d.Type)
	step.Name = "LocalReduce"
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		isFirst := true
		var localResult reflect.Value
		fn := reflect.ValueOf(f)
		for input := range task.InputChan() {
			if isFirst {
				isFirst = false
				localResult = input
			} else {
				outs := fn.Call([]reflect.Value{
					localResult,
					input,
				})
				localResult = outs[0]
			}
		}
		outChan.Send(localResult)
	}
	return ret
}

func (d *Dataset) MergeReduce(f interface{}) (ret *Dataset) {
	ret = d.context.newNextDataset(1, d.Type)
	step := d.context.AddAllToOneStep(d, ret)
	step.Name = "MergeReduce"
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		isFirst := true
		var localResult reflect.Value
		fn := reflect.ValueOf(f)
		for input := range task.MergedInputChan() {
			if isFirst {
				isFirst = false
				localResult = input
			} else {
				outs := fn.Call([]reflect.Value{
					localResult,
					input,
				})
				localResult = outs[0]
			}
		}
		outChan.Send(localResult)
	}
	return ret
}

func (d *Dataset) LocalReduceByKey(f interface{}) *Dataset {
	ret, step := add1ShardTo1Step(d, d.Type)
	step.Name = "LocalReduceByKey"
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		foldSameKey(task.InputChan(), f, outChan)
	}
	return ret
}

func foldSameKey(inputs chan reflect.Value, f interface{}, outChan reflect.Value) {
	var prevKey interface{}
	fn := reflect.ValueOf(f)
	var localResult reflect.Value
	for input := range inputs {
		kv := input.Interface().(KeyValue)
		if !reflect.DeepEqual(prevKey, kv.Key) {
			if localResult.IsValid() {
				sendKeyValue(outChan, prevKey, localResult.Interface())
			}
			prevKey = kv.Key
			localResult = reflect.ValueOf(kv.Value)
		} else {
			outs := fn.Call([]reflect.Value{
				localResult,
				reflect.ValueOf(kv.Value),
			})
			localResult = outs[0]
		}
	}
	sendKeyValue(outChan, prevKey, localResult.Interface())
}
