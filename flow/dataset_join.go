package flow

import (
	"reflect"
)

// assume nothing about these two dataset
func (d *Dataset) Join(other *Dataset) *Dataset {
	sorted_d := d.Partition(len(d.Shards)).LocalSort(nil)
	if d == other {
		return sorted_d.SelfJoin(nil)
	}
	sorted_other := other.Partition(len(d.Shards)).LocalSort(nil)
	return sorted_d.JoinPartitionedSorted(sorted_other, nil, false, false)
}

// Join multiple datasets that are sharded by the same key, and locally sorted within the shard
func (this *Dataset) JoinPartitionedSorted(that *Dataset,
	compareFunc interface{}, isLeftOuterJoin, isRightOuterJoin bool,
) (ret *Dataset) {
	outType := KeyValueValueType
	ret = this.context.newNextDataset(len(this.Shards), outType)

	inputs := []*Dataset{this, that}
	step := this.context.MergeDatasets1ShardTo1Step(inputs, ret)
	step.Name = "JoinPartitionedSorted"
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

		var leftValues, rightValues []interface{}
		var leftNextKey, leftNextValue, rightNextKey, rightNextValue interface{}
		for leftHasValue && rightHasValue {
			x := comparator(leftKey, rightKey)
			switch {
			case x == 0:
				leftNextKey, leftNextValue, leftValues, leftHasValue = getSameKeyValues(leftChan, comparator, leftKey, leftValue, leftHasValue)
				rightNextKey, rightNextValue, rightValues, rightHasValue = getSameKeyValues(rightChan, comparator, rightKey, rightValue, rightHasValue)

				// fmt.Printf("left %+v, %v ============ right %+v %v\n", leftKey, leftValues, rightKey, rightValues)
				// left and right cartician join
				for _, a := range leftValues {
					for _, b := range rightValues {
						sendKeyValueValue(outChan, leftKey, a, b)
					}
				}
				leftKey, leftValue, rightKey, rightValue = leftNextKey, leftNextValue, rightNextKey, rightNextValue
			case x < 0:
				if isLeftOuterJoin {
					sendKeyValueValue(outChan, leftKey, leftValue, nil)
				}
				leftKey, leftValue, leftHasValue = getKeyValue(leftChan)
			case x > 0:
				if isRightOuterJoin {
					sendKeyValueValue(outChan, rightKey, nil, rightValue)
				}
				rightKey, rightValue, rightHasValue = getKeyValue(rightChan)
			}
		}
		if leftHasValue {
			if isLeftOuterJoin {
				sendKeyValueValue(outChan, leftKey, leftValue, nil)
			}
		}
		for leftKeyValue := range leftChan {
			if isLeftOuterJoin {
				leftKey, leftValue = leftKeyValue.Field(0), leftKeyValue.Field(1)
				sendKeyValueValue(outChan, leftKey, leftValue, nil)
			}
		}
		if rightHasValue {
			if isRightOuterJoin {
				sendKeyValueValue(outChan, rightKey, nil, rightValue)
			}
		}
		for rightKeyValue := range rightChan {
			if isRightOuterJoin {
				rightKey, rightValue = rightKeyValue.Field(0), rightKeyValue.Field(1)
				sendKeyValueValue(outChan, rightKey, nil, rightValue)
			}
		}

	}
	return ret
}

func (d *Dataset) SelfJoin(compareFunc interface{}) (ret *Dataset) {
	outType := KeyValueValueType
	ret, step := add1ShardTo1Step(d, outType)
	step.Name = "SelfJoin"
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan

		leftChan := task.InputChans[0]

		// get first value from both channels
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

		var leftValues []interface{}
		for leftHasValue {
			leftKey, leftValue, leftValues, leftHasValue = getSameKeyValues(leftChan, comparator, leftKey, leftValue, leftHasValue)

			// cartician join
			if leftHasValue {
				for _, a := range leftValues {
					for _, b := range leftValues {
						if a != nil && b != nil {
							sendKeyValueValue(outChan, leftKey, a, b)
						}
					}
				}
			}
		}

	}
	return ret
}

func sendKeyValue(outChan reflect.Value, key, value interface{}) {
	outChan.Send(reflect.ValueOf(KeyValue{key, value}))
}

func sendKeyValueValue(outChan reflect.Value, key, a, b interface{}) {
	outChan.Send(reflect.ValueOf(KeyValueValue{key, a, b}))
}
