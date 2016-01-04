package flow

import (
	"reflect"
)

func (d *Dataset) GroupByKey() *Dataset {
	sorted_d := d.Partition(len(d.Shards)).LocalSort(nil)

	return sorted_d.LocalGroupByKey(nil)
}

func (d *Dataset) LocalGroupByKey(compareFunc interface{}) *Dataset {
	ret, step := add1ShardTo1Step(d, KeyValuesType)
	step.Name = "LocalGroupByKey"
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan

		leftChan := task.InputChans[0]

		// get first value
		leftKey, leftValue, leftHasValue := getKeyValue(leftChan)

		// get comparator
		if compareFunc == nil {
			if leftHasValue {
				compareFunc = getComparator(reflect.TypeOf(leftKey))
			}
		}
		fn := reflect.ValueOf(compareFunc)
		comparator := func(a, b interface{}) int64 {
			outs := fn.Call([]reflect.Value{
				reflect.ValueOf(a),
				reflect.ValueOf(b),
			})
			return outs[0].Int()
		}

		var valueType reflect.Type
		if leftHasValue {
			valueType = reflect.TypeOf(leftValue)
		}

		var leftValues []interface{}
		var leftNextKey, leftNextValue interface{}
		for leftHasValue {
			leftNextKey, leftNextValue, leftValues, leftHasValue = getSameKeyValues(leftChan, comparator, leftKey, leftValue, leftHasValue)
			sendKeyValues(outChan, leftKey, valueType, leftValues)
			leftKey, leftValue = leftNextKey, leftNextValue
		}

	}
	return ret
}

func (d *Dataset) CoGroup(other *Dataset) *Dataset {
	sorted_d := d.Partition(len(d.Shards)).LocalSort(nil)
	if d == other {
		// this should not happen, but just in case
		return sorted_d.LocalGroupByKey(nil)
	}
	sorted_other := other.Partition(len(d.Shards)).LocalSort(nil)
	return sorted_d.CoGroupPartitionedSorted(sorted_other, nil)
}

// CoGroupPartitionedSorted joins 2 datasets that are sharded
// by the same key and already locally sorted within each shard.
func (this *Dataset) CoGroupPartitionedSorted(that *Dataset,
	compareFunc interface{}) (ret *Dataset) {
	ret = this.context.newNextDataset(len(this.Shards), KeyValuesValuesType)

	inputs := []*Dataset{this, that}
	step := this.context.MergeDatasets1ShardTo1Step(inputs, ret)
	step.Name = "CoGroupPartitionedSorted"
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan

		leftChan := newChannelOfValuesWithSameKey(task.InputChans[0], compareFunc)
		rightChan := newChannelOfValuesWithSameKey(task.InputChans[1], compareFunc)

		// get first value from both channels
		leftValuesWithSameKey, leftHasValue := <-leftChan
		rightValuesWithSameKey, rightHasValue := <-rightChan

		if compareFunc == nil {
			if leftHasValue {
				compareFunc = getComparator(reflect.TypeOf(leftValuesWithSameKey.Key))
			} else if rightHasValue {
				compareFunc = getComparator(reflect.TypeOf(rightValuesWithSameKey.Key))
			}
		}
		fn := reflect.ValueOf(compareFunc)
		comparator := func(a, b interface{}) int64 {
			outs := fn.Call([]reflect.Value{
				reflect.ValueOf(a),
				reflect.ValueOf(b),
			})
			return outs[0].Int()
		}

		var leftType, rightType reflect.Type
		if leftHasValue {
			leftType = reflect.TypeOf(leftValuesWithSameKey.Values[0])
		}
		if rightHasValue {
			rightType = reflect.TypeOf(rightValuesWithSameKey.Values[0])
		}

		for leftHasValue && rightHasValue {
			x := comparator(leftValuesWithSameKey.Key, rightValuesWithSameKey.Key)
			switch {
			case x == 0:
				sendKeyValuesValues(outChan, leftValuesWithSameKey.Key, leftType, leftValuesWithSameKey.Values, rightType, rightValuesWithSameKey.Values)
				leftValuesWithSameKey, leftHasValue = <-leftChan
				rightValuesWithSameKey, rightHasValue = <-rightChan
			case x < 0:
				sendKeyValuesValues(outChan, leftValuesWithSameKey.Key, leftType, leftValuesWithSameKey.Values, rightType, []interface{}{})
				leftValuesWithSameKey, leftHasValue = <-leftChan
			case x > 0:
				sendKeyValuesValues(outChan, rightValuesWithSameKey.Key, leftType, []interface{}{}, rightType, rightValuesWithSameKey.Values)
				rightValuesWithSameKey, rightHasValue = <-rightChan
			}
		}
		for leftHasValue {
			sendKeyValuesValues(outChan, leftValuesWithSameKey.Key, leftType, leftValuesWithSameKey.Values, rightType, []interface{}{})
			leftValuesWithSameKey, leftHasValue = <-leftChan
		}
		for rightHasValue {
			sendKeyValuesValues(outChan, rightValuesWithSameKey.Key, leftType, []interface{}{}, rightType, rightValuesWithSameKey.Values)
			rightValuesWithSameKey, rightHasValue = <-rightChan
		}
	}
	return ret
}

func sendKeyValuesValues(outChan reflect.Value, key interface{},
	leftType reflect.Type, leftValues []interface{}, rightType reflect.Type, rightValues []interface{}) {

	slice1Len := len(leftValues)
	slice1Value := reflect.MakeSlice(reflect.SliceOf(leftType), slice1Len, slice1Len)
	for i, value := range leftValues {
		slice1Value.Index(i).Set(reflect.ValueOf(value))
	}

	slice2Len := len(rightValues)
	slice2Value := reflect.MakeSlice(reflect.SliceOf(rightType), slice2Len, slice2Len)
	for i, value := range rightValues {
		slice2Value.Index(i).Set(reflect.ValueOf(value))
	}

	outChan.Send(reflect.ValueOf(KeyValuesValues{key, slice1Value.Interface(), slice2Value.Interface()}))
}

func sendKeyValues(outChan reflect.Value, key interface{}, valueType reflect.Type, values []interface{}) {
	sliceElementType := reflect.SliceOf(valueType)

	sliceLen := len(values)
	sliceValue := reflect.MakeSlice(sliceElementType, sliceLen, sliceLen)
	for i, value := range values {
		sliceValue.Index(i).Set(reflect.ValueOf(value))
	}

	outChan.Send(reflect.ValueOf(KeyValues{key, sliceValue.Interface()}))
}
