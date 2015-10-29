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
// by the same key, and locally sorted within the shard
func (this *Dataset) CoGroupPartitionedSorted(that *Dataset,
	compareFunc interface{}) (ret *Dataset) {
	ret = this.context.newNextDataset(len(this.Shards), KeyValuesValuesType)

	inputs := []*Dataset{this, that}
	step := this.context.MergeDatasets1ShardTo1Step(inputs, ret)
	step.Name = "CoGroupPartitionedSorted"
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan

		leftChan := task.InputChans[0]
		rightChan := task.InputChans[1]

		// get first value from both channels
		leftKey, leftValue, leftHasValue := getKeyValue(leftChan)
		rightKey, rightValue, rightHasValue := getKeyValue(rightChan)

		if compareFunc == nil {
			if leftHasValue {
				compareFunc = getComparator(reflect.TypeOf(leftKey))
			} else if rightHasValue {
				compareFunc = getComparator(reflect.TypeOf(rightKey))
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
		} else if rightHasValue {
			valueType = reflect.TypeOf(rightValue)
		}

		var leftValues, rightValues []interface{}
		var leftNextKey, leftNextValue, rightNextKey, rightNextValue interface{}
		for leftHasValue && rightHasValue {
			x := comparator(leftKey, rightKey)
			switch {
			case x == 0:
				leftNextKey, leftNextValue, leftValues, leftHasValue = getSameKeyValues(leftChan, comparator, leftKey, leftValue, leftHasValue)
				rightNextKey, rightNextValue, rightValues, rightHasValue = getSameKeyValues(rightChan, comparator, rightKey, rightValue, rightHasValue)
				sendKeyValuesValues(outChan, leftKey, valueType, leftValues, rightValues)
				leftKey, leftValue, rightKey, rightValue = leftNextKey, leftNextValue, rightNextKey, rightNextValue
			case x < 0:
				leftNextKey, leftNextValue, leftValues, leftHasValue = getSameKeyValues(leftChan, comparator, leftKey, leftValue, leftHasValue)
				sendKeyValuesValues(outChan, leftKey, valueType, leftValues, []interface{}{})
				leftKey, leftValue = leftNextKey, leftNextValue
			case x > 0:
				rightNextKey, rightNextValue, rightValues, rightHasValue = getSameKeyValues(rightChan, comparator, rightKey, rightValue, rightHasValue)
				sendKeyValuesValues(outChan, rightKey, valueType, []interface{}{}, rightValues)
				rightKey, rightValue = rightNextKey, rightNextValue
			}
		}
		for leftHasValue {
			leftNextKey, leftNextValue, leftValues, leftHasValue = getSameKeyValues(leftChan, comparator, leftKey, leftValue, leftHasValue)
			sendKeyValuesValues(outChan, leftKey, valueType, leftValues, []interface{}{})
			leftKey, leftValue = leftNextKey, leftNextValue
		}
		for rightHasValue {
			rightNextKey, rightNextValue, rightValues, rightHasValue = getSameKeyValues(rightChan, comparator, rightKey, rightValue, rightHasValue)
			sendKeyValuesValues(outChan, rightKey, valueType, []interface{}{}, rightValues)
			rightKey, rightValue = rightNextKey, rightNextValue
		}
	}
	return ret
}

func sendKeyValuesValues(outChan reflect.Value, key interface{}, valueType reflect.Type, values1, values2 []interface{}) {
	sliceType := reflect.SliceOf(valueType)

	slice1Len := len(values1)
	slice1Value := reflect.MakeSlice(sliceType, slice1Len, slice1Len)
	for i, value := range values1 {
		slice1Value.Index(i).Set(reflect.ValueOf(value))
	}

	slice2Len := len(values2)
	slice2Value := reflect.MakeSlice(sliceType, slice2Len, slice2Len)
	for i, value := range values2 {
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
