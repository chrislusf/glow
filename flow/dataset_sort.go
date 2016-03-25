package flow

import (
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
	if f == nil && d.IsKeyLocalSorted {
		return d
	}
	ret, step := add1ShardTo1Step(d, d.Type)
	ret.IsKeyPartitioned = d.IsKeyPartitioned
	if f == nil {
		ret.IsKeyLocalSorted = true
	}
	step.Name = "LocalSort"
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan
		var kvs []interface{}
		for input := range task.InputChan() {
			kvs = append(kvs, input.Interface())
		}
		if len(kvs) == 0 {
			return
		}
		comparator := getLessThanComparator(d.Type, reflect.ValueOf(kvs[0]), f)
		timsort.Sort(kvs, comparator)

		for _, kv := range kvs {
			outChan.Send(reflect.ValueOf(kv))
			// println(task.Name(), "sent kv index:", i)
		}
	}
	return ret
}

func (d *Dataset) MergeSorted(f interface{}) (ret *Dataset) {
	ret = d.context.newNextDataset(1, d.Type)
	step := d.context.AddAllToOneStep(d, ret)
	step.Name = "MergeSorted"
	step.Function = func(task *Task) {
		outChan := task.Outputs[0].WriteChan

		var pq *util.PriorityQueue
		// enqueue one item to the pq from each shard
		isFirst := true
		for shardId, shardChan := range task.InputChans {
			if x, ok := <-shardChan; ok {
				if isFirst {
					isFirst = false
					v := guessKey(x)
					comparator := getLessThanComparator(d.Type, v, f)
					pq = util.NewPriorityQueue(comparator)
				}
				pq.Enqueue(x.Interface(), shardId)
			}
		}
		if pq == nil {
			return
		}
		for pq.Len() > 0 {
			t, shardId := pq.Dequeue()
			outChan.Send(reflect.ValueOf(t))
			if x, ok := <-task.InputChans[shardId]; ok {
				pq.Enqueue(x.Interface(), shardId)
			}
		}
	}
	return ret
}
