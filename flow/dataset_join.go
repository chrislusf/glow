package flow

import (
	"reflect"
)

// assume nothing about these two dataset
func (d *Dataset) Join(other *Dataset) *Dataset {
	sorted_d := d.Partition(len(d.Shards)).LocalSort(nil)
	var sorted_other *Dataset
	if d == other {
		sorted_other = sorted_d
	} else {
		sorted_other = other.Partition(len(d.Shards)).LocalSort(nil)
	}
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

		for leftHasValue && rightHasValue {
			x := comparator(leftValuesWithSameKey.Key, rightValuesWithSameKey.Key)
			switch {
			case x == 0:
				// left and right cartician join
				for _, a := range leftValuesWithSameKey.Values {
					for _, b := range rightValuesWithSameKey.Values {
						sendKeyValueValue(outChan, leftValuesWithSameKey.Key, a, b)
					}
				}
				leftValuesWithSameKey, leftHasValue = <-leftChan
				rightValuesWithSameKey, rightHasValue = <-rightChan
			case x < 0:
				if isLeftOuterJoin {
					for _, leftValue := range leftValuesWithSameKey.Values {
						sendKeyValueValue(outChan, leftValuesWithSameKey.Key, leftValue, nil)
					}
				}
				leftValuesWithSameKey, leftHasValue = <-leftChan
			case x > 0:
				if isRightOuterJoin {
					for _, rightValue := range rightValuesWithSameKey.Values {
						sendKeyValueValue(outChan, rightValuesWithSameKey.Key, nil, rightValue)
					}
				}
				rightValuesWithSameKey, rightHasValue = <-rightChan
			}
		}
		if leftHasValue {
			if isLeftOuterJoin {
				for _, leftValue := range leftValuesWithSameKey.Values {
					sendKeyValueValue(outChan, leftValuesWithSameKey.Key, leftValue, nil)
				}
			}
		}
		for leftValuesWithSameKey = range leftChan {
			if isLeftOuterJoin {
				for _, leftValue := range leftValuesWithSameKey.Values {
					sendKeyValueValue(outChan, leftValuesWithSameKey.Key, leftValue, nil)
				}
			}
		}
		if rightHasValue {
			if isRightOuterJoin {
				for _, rightValue := range rightValuesWithSameKey.Values {
					sendKeyValueValue(outChan, rightValuesWithSameKey.Key, nil, rightValue)
				}
			}
		}
		for rightValuesWithSameKey = range rightChan {
			if isRightOuterJoin {
				for _, rightValue := range rightValuesWithSameKey.Values {
					sendKeyValueValue(outChan, rightValuesWithSameKey.Key, nil, rightValue)
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
