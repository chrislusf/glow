package flow

import (
	"fmt"
	"reflect"
	"sync"
)

func (d *Dataset) GetShards() []*DatasetShard {
	return d.Shards
}

type Dataset struct {
	Id      int
	context *FlowContext
	Type    reflect.Type
	Shards  []*DatasetShard
	Step    *Step
}

type DatasetShard struct {
	Id        int
	Parent    *Dataset
	ReadChan  chan reflect.Value
	WriteChan reflect.Value
}

func NewDataset(context *FlowContext, t reflect.Type) *Dataset {
	d := &Dataset{
		Id:      len(context.Datasets),
		context: context,
		Type:    t,
	}
	context.Datasets = append(context.Datasets, d)
	return d
}

func (d *Dataset) RunSelf(stepId int) {
	var wg sync.WaitGroup
	for shardId, shard := range d.Shards {
		wg.Add(1)
		go func(shardId int, shard *DatasetShard) {
			defer wg.Done()
			var t reflect.Value
			for ok := true; ok; {
				if t, ok = shard.WriteChan.Recv(); ok {
					// fmt.Printf("%s -> r\n", t)
					shard.ReadChan <- t
				}
			}
			// println("dataset", stepId, "shard", shardId, "close r")
			close(shard.ReadChan)
		}(shardId, shard)
	}
	wg.Wait()
	// println("dataset", stepId, "stopped")
	return
}

func (s *DatasetShard) Name() string {
	return fmt.Sprintf("ct-%d-ds-%d-shard-%d", s.Parent.context.Id, s.Parent.Id, s.Id)
}
