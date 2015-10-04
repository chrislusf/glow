package flow

import (
	"log"
	"reflect"
)

func DefaultStringComparator(a, b string) int64 {
	switch {
	case a == b:
		return 0
	case a < b:
		return -1
	default:
		return 1
	}
}
func DefaultInt64Comparator(a, b int64) int64 {
	return a - b
}
func DefaultFloat64Comparator(a, b float64) int64 {
	switch {
	case a == b:
		return 0
	case a < b:
		return -1
	default:
		return 1
	}
}

func getComparator(dt reflect.Type) (funcPointer interface{}) {
	switch dt.Kind() {
	case reflect.Int:
		funcPointer = DefaultInt64Comparator
	case reflect.Float64:
		funcPointer = DefaultFloat64Comparator
	case reflect.String:
		funcPointer = DefaultStringComparator
	default:
		log.Panicf("No default comparator for %s:%s", dt.String(), dt.Kind().String())
	}
	return
}

// assume nothing about these two dataset
func (d *Dataset) Join(other *Dataset) *Dataset {
	d = d.Partition(len(d.Shards)).LocalSort(nil)
	other = other.Partition(len(d.Shards)).LocalSort(nil)
	return d.JoinHashedSorted(other, nil, false, false)
}

// Join multiple datasets that are sharded by the same key, and locally sorted within the shard
// "this" dataset is the driving dataset and should have more data than "that"
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

		leftChan := task.Inputs[0].ReadChan
		rightChan := task.Inputs[1].ReadChan

		// get first value from both channels
		rightKey, rightValue, rightHasValue := getKeyValue(rightChan)
		leftKey, leftValue, leftHasValue := getKeyValue(leftChan)

		if compareFunc == nil {
			compareFunc = getComparator(reflect.TypeOf(leftKey))
		}
		fn := reflect.ValueOf(compareFunc)
		comparator := func(a, b interface{}) int64 {
			outs := fn.Call([]reflect.Value{
				reflect.ValueOf(a),
				reflect.ValueOf(b),
			})
			return outs[0].Int()
		}

		for leftHasValue {
			if !rightHasValue {
				if isLeftOuterJoin {
					// outChan.Send(reflect.ValueOf(NewTuple3(leftKey, leftValue, nil)))
					send(outChan, leftKey, leftValue, nil)
				}
				leftKey, leftValue, leftHasValue = getKeyValue(leftChan)
				continue
			}

			x := comparator(leftKey, rightKey)
			switch {
			case x == 0:
				// collect all values on rightChan
				rightValues := []interface{}{rightValue}
				prevRightKey := rightKey
				for {
					rightKey, rightValue, rightHasValue = getKeyValue(rightChan)
					if rightHasValue && comparator(prevRightKey, rightKey) == 0 {
						rightValues = append(rightValues, rightValue)
						continue
					} else {
						// for current left key, join with all rightValues
						for _, rv := range rightValues {
							// outChan.Send(reflect.ValueOf(NewTuple3(leftKey, leftValue, rv)))
							send(outChan, leftKey, leftValue, rv)
						}
						// reset right loop
						rightValues = rightValues[0:0]
						leftKey, leftValue, leftHasValue = getKeyValue(leftChan)
						break
					}
				}
			case x < 0:
				if isLeftOuterJoin {
					// outChan.Send(reflect.ValueOf(NewTuple3(leftKey, leftValue, nil)))
					send(outChan, leftKey, leftValue, nil)
				}
				leftKey, leftValue, leftHasValue = getKeyValue(leftChan)
			case x > 0:
				if isRightOuterJoin {
					// outChan.Send(reflect.ValueOf(NewTuple3(rightKey, nil, rightValue)))
					send(outChan, rightKey, nil, rightValue)
				}
				rightKey, rightValue, rightHasValue = getKeyValue(rightChan)
			}
		}
		// handle right outer join tail case
		for rightKeyValue := range rightChan {
			if isRightOuterJoin {
				rightKey, rightValue := rightKeyValue.Field(0), rightKeyValue.Field(1)
				// outChan.Send(reflect.ValueOf(NewTuple3(rightKey, nil, rightValue)))
				send(outChan, rightKey, nil, rightValue)
			}
		}

	}
	return ret
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
