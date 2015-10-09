package flow

import (
	"reflect"
	"sync"
)

func (d *Dataset) GetShards() []*DatasetShard {
	return d.Shards
}

type Dataset struct {
	Id           int
	context      *FlowContext
	Type         reflect.Type
	Shards       []*DatasetShard
	Step         *Step
	ReadingSteps []*Step
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
	for _, shard := range d.Shards {
		wg.Add(1)
		go func(shard *DatasetShard) {
			defer wg.Done()
			shard.SetupReadingChans()

			// start to run
			var t reflect.Value
			for ok := true; ok; {
				if t, ok = shard.WriteChan.Recv(); ok {
					shard.SendForRead(t)
				}
			}
			shard.CloseRead()
		}(shard)
	}
	wg.Wait()
	// println("dataset", stepId, "stopped")
	return
}
