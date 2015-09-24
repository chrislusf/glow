package flame

import (
	"reflect"

	"github.com/chrislusf/glow/util"
)

func (d *Dataset) SetupShard(n int) {
	ctype := reflect.ChanOf(reflect.BothDir, d.Type)
	for i := 0; i < n; i++ {
		ds := &DatasetShard{
			Id:        i,
			Parent:    d,
			ReadChan:  make(chan reflect.Value, 0),
			WriteChan: reflect.MakeChan(ctype, 0),
		}
		d.Shards = append(d.Shards, ds)
	}
}

// hash data or by data key, return a new dataset
// This is devided into 2 steps:
// 1. Each record is sharded to a local shard
// 2. The destination shard will collect its child shards and merge into one
func (d *Dataset) Partition(shard int) *Dataset {
	if shard == 1 {
		return d
	}
	return d.partition_scatter(shard).partition_collect(shard)
}

func HashByKey(input reflect.Value, shard int) int {
	v := guessKey(input)

	dt := v.Type()
	if dt.Kind() == reflect.Interface {
		dt = reflect.TypeOf(v.Interface())
	}

	var x int
	switch dt.Kind() {
	case reflect.Int:
		x = int(v.Int()) % shard
	case reflect.String:
		x = int(util.Hash([]byte(v.String()))) % shard
	case reflect.Slice:
		x = int(util.Hash(v.Bytes())) % shard
	default:
		println("unexpected key to hash:", v.Kind().String())
	}
	return x
}

func (d *Dataset) partition_scatter(shard int) (ret *Dataset) {
	ret = d.context.newNextDataset(len(d.Shards)*shard, d.Type)
	step := d.context.AddOneToEveryNStep(d, shard, ret)
	step.Function = func(task *Task) {
		for input := range task.InputChan() {
			x := HashByKey(input, shard)
			task.Outputs[x].WriteChan.Send(input)
		}
	}
	return
}

func (d *Dataset) partition_collect(shard int) (ret *Dataset) {
	m := len(d.Shards) / shard
	ret = d.context.newNextDataset(shard, d.Type)
	step := d.context.AddEveryNToOneStep(d, m, ret)
	step.Function = func(task *Task) {
		for input := range task.InputChan() {
			task.Outputs[0].WriteChan.Send(input)
		}
	}
	return
}
