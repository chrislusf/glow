package flow

import (
	"reflect"
)

// Reduce runs on a dataset with data type V.
// Function f takes two arguments of type V and returns one type V.
// The function should be commutative and associative
// so that it can be computed correctly in parallel.
func (d *Dataset) Reduce(f interface{}) (ret *Dataset) {
	return d.LocalReduce(f).MergeReduce(f)
}

func (d *Dataset) LocalReduce(f interface{}) *Dataset {
	ret, step := add1ShardTo1Step(d, d.Type)
	step.Name = "LocalReduce"
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		hasValue := false
		var localResult reflect.Value
		fn := reflect.ValueOf(f)
		for input := range task.InputChan() {
			if !hasValue {
				hasValue = true
				localResult = input
			} else {
				outs := fn.Call([]reflect.Value{
					localResult,
					input,
				})
				localResult = outs[0]
			}
		}
		if hasValue {
			outChan.Send(localResult)
		}
	}
	return ret
}

func (d *Dataset) MergeReduce(f interface{}) (ret *Dataset) {
	ret = d.context.newNextDataset(1, d.Type)
	step := d.context.AddAllToOneStep(d, ret)
	step.Name = "MergeReduce"
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		hasValue := false
		var localResult reflect.Value
		fn := reflect.ValueOf(f)
		for input := range task.MergedInputChan() {
			if !hasValue {
				hasValue = true
				localResult = input
			} else {
				outs := fn.Call([]reflect.Value{
					localResult,
					input,
				})
				localResult = outs[0]
			}
		}
		if hasValue {
			outChan.Send(localResult)
		}
	}
	return ret
}

// ReduceByKey runs on a dataset with (K, V) pairs,
// returns a dataset with (K, V) pairs, where values for the same key
// are aggreated by function f.
// Function f takes two arguments of type V and returns one type V.
// The function should be commutative and associative
// so that it can be computed correctly in parallel.
func (d *Dataset) ReduceByKey(f interface{}) *Dataset {
	return d.LocalSort(nil).LocalReduceByKey(f).MergeSorted(nil).LocalReduceByKey(f)
}

func (d *Dataset) ReduceByUserDefinedKey(lessThanFunc interface{}, reducer interface{}) *Dataset {
	return d.LocalSort(lessThanFunc).LocalReduceByKey(reducer).MergeSorted(lessThanFunc).LocalReduceByKey(reducer)
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
	hasValue := false
	for input := range inputs {
		kv := input.Interface().(KeyValue)
		if !hasValue {
			hasValue = true
			prevKey = kv.Key
			localResult = reflect.ValueOf(kv.Value)
		} else if !reflect.DeepEqual(prevKey, kv.Key) {
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
	if hasValue {
		sendKeyValue(outChan, prevKey, localResult.Interface())
	}
}
