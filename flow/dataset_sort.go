package flow

import (
	"log"
	"reflect"

	"github.com/chrislusf/glow/util"
	"github.com/psilva261/timsort"
)

func (d *Dataset) Sort(f interface{}) (ret *Dataset) {
	return d.LocalSort(f).MergeSorted(f)
}

// f(V, V) bool : less than function
// New Dataset contains K,V
func (d *Dataset) LocalSort(f interface{}) *Dataset {
	ret, step := add1ShardTo1Step(d, d.Type)
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		var kvs []interface{}
		for input := range task.InputChan() {
			kvs = append(kvs, input.Interface())
		}
		if len(kvs) == 0 {
			return
		}
		v := guessKey(reflect.ValueOf(kvs[0]))
		comparator := getLessThanComparator(d.Type, v, f)
		timsort.Sort(kvs, comparator)

		for _, kv := range kvs {
			outChan.Send(reflect.ValueOf(kv))
		}
	}
	return ret
}

func (d *Dataset) MergeSorted(f interface{}) (ret *Dataset) {
	ret = d.context.newNextDataset(1, d.Type)
	step := d.context.AddAllToOneStep(d, ret)
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan

		var pq *util.PriorityQueue
		// enqueue one item to the pq from each shard
		isFirst := true
		for shardId, shard := range task.Inputs {
			if x, ok := <-shard.ReadChan; ok {
				if isFirst {
					isFirst = false
					v := guessKey(x)
					comparator := getLessThanComparator(d.Type, v, f)
					pq = util.NewPriorityQueue(comparator)
				}
				pq.Enqueue(x.Interface(), shardId)
			}
		}
		for pq.Len() > 0 {
			t, shardId := pq.Dequeue()
			outChan.Send(reflect.ValueOf(t))
			if x, ok := <-task.Inputs[shardId].ReadChan; ok {
				pq.Enqueue(x.Interface(), shardId)
			}
		}
	}
	return ret
}

func DefaultStringLessThanComparator(a, b string) bool {
	return a < b
}
func DefaultInt64LessThanComparator(a, b int64) bool {
	return a < b
}
func DefaultFloat64LessThanComparator(a, b float64) bool {
	return a < b
}

func _getLessThanComparatorByKeyValue(key reflect.Value) (funcPointer interface{}) {
	dt := key.Type()
	if key.Kind() == reflect.Interface {
		dt = reflect.TypeOf(key.Interface())
	}
	switch dt.Kind() {
	case reflect.Int:
		funcPointer = DefaultInt64LessThanComparator
	case reflect.Float64:
		funcPointer = DefaultFloat64LessThanComparator
	case reflect.String:
		funcPointer = DefaultStringLessThanComparator
	default:
		log.Panicf("No default less than comparator for type:%s, kind:%s", dt.String(), dt.Kind().String())
	}
	return
}

func getLessThanComparator(datasetType reflect.Type, key reflect.Value, functionPointer interface{}) func(a interface{}, b interface{}) bool {
	lessThanFuncValue := reflect.ValueOf(functionPointer)
	if functionPointer == nil {
		v := guessKey(key)
		lessThanFuncValue = reflect.ValueOf(_getLessThanComparatorByKeyValue(v))
	}
	if datasetType.Kind() == reflect.Slice {
		return func(a interface{}, b interface{}) bool {
			// println("a:", reflect.ValueOf(a).Field(0).Kind().String(), "lessThanFuncValue:", lessThanFuncValue.String())
			ret := lessThanFuncValue.Call([]reflect.Value{
				reflect.ValueOf(reflect.ValueOf(a).Index(0).Interface()),
				reflect.ValueOf(reflect.ValueOf(b).Index(0).Interface()),
			})
			return ret[0].Bool()
		}
	} else {
		return func(a interface{}, b interface{}) bool {
			ret := lessThanFuncValue.Call([]reflect.Value{
				reflect.ValueOf(a),
				reflect.ValueOf(b),
			})
			return ret[0].Bool()
		}
	}
}
