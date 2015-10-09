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
	return sorted_d.JoinHashedSorted(sorted_other, nil, false, false)
}

// Join multiple datasets that are sharded by the same key, and locally sorted within the shard
func (this *Dataset) JoinHashedSorted(that *Dataset,
	compareFunc interface{}, isLeftOuterJoin, isRightOuterJoin bool,
) (ret *Dataset) {
	outType := reflect.TypeOf([]interface{}{})
	ret = this.context.newNextDataset(len(this.Shards), outType)

	inputs := []*Dataset{this, that}
	step := this.context.MergeDatasets1ShardTo1Step(inputs, ret)
	step.Name = "JoinHashedSorted"
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
						send(outChan, leftKey, a, b)
					}
				}
				leftKey, leftValue, rightKey, rightValue = leftNextKey, leftNextValue, rightNextKey, rightNextValue
			case x < 0:
				if isLeftOuterJoin {
					send(outChan, leftKey, leftValue, nil)
				}
				leftKey, leftValue, leftHasValue = getKeyValue(leftChan)
			case x > 0:
				if isRightOuterJoin {
					send(outChan, rightKey, nil, rightValue)
				}
				rightKey, rightValue, rightHasValue = getKeyValue(rightChan)
			}
		}
		if leftHasValue {
			if isLeftOuterJoin {
				send(outChan, leftKey, leftValue, nil)
			}
		}
		for leftKeyValue := range leftChan {
			if isLeftOuterJoin {
				leftKey, leftValue = leftKeyValue.Field(0), leftKeyValue.Field(1)
				send(outChan, leftKey, leftValue, nil)
			}
		}
		if rightHasValue {
			if isRightOuterJoin {
				send(outChan, rightKey, nil, rightValue)
			}
		}
		for rightKeyValue := range rightChan {
			if isRightOuterJoin {
				rightKey, rightValue = rightKeyValue.Field(0), rightKeyValue.Field(1)
				send(outChan, rightKey, nil, rightValue)
			}
		}

	}
	return ret
}

func getSameKeyValues(ch chan reflect.Value, comparator func(a, b interface{}) int64, theKey, firstValue interface{}, hasFirstValue bool) (nextKey, nextValue interface{}, theValues []interface{}, hasValue bool) {
	theValues = append(theValues, firstValue)
	hasValue = hasFirstValue
	for {
		nextKey, nextValue, hasValue = getKeyValue(ch)
		if hasValue && comparator(theKey, nextKey) == 0 {
			theValues = append(theValues, nextValue)
		} else {
			return
		}
	}
	return
}

func getKeyValue(ch chan reflect.Value) (key, value interface{}, ok bool) {
	keyValue, hasValue := <-ch
	if hasValue {
		key = keyValue.Index(0).Interface()
		value = keyValue.Index(1).Interface()
	}
	return key, value, hasValue
}

func send(outChan reflect.Value, values ...interface{}) {
	outChan.Send(reflect.ValueOf(values))
}

func (d *Dataset) SelfJoin(compareFunc interface{}) (ret *Dataset) {
	outType := reflect.TypeOf([]interface{}{})
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
							send(outChan, leftKey, a, b)
						}
					}
				}
			}
		}

	}
	return ret
}
