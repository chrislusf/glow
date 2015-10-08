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
	Id           int
	context      *FlowContext
	Type         reflect.Type
	Shards       []*DatasetShard
	Step         *Step
	ReadingSteps []*Step
}

type DatasetShard struct {
	Id           int
	Parent       *Dataset
	ReadChan     chan reflect.Value
	WriteChan    reflect.Value
	ReadingTasks []*Task
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
					shard.SendForRead(t)
				}
			}
			shard.CloseRead()
		}(shardId, shard)
	}
	wg.Wait()
	// println("dataset", stepId, "stopped")
	return
}

func (s *DatasetShard) Name() string {
	return fmt.Sprintf("ct-%d-ds-%d-shard-%d", s.Parent.context.Id, s.Parent.Id, s.Id)
}

func (s *DatasetShard) SendForRead(t reflect.Value) {
	s.ReadChan <- t
}

func (s *DatasetShard) CloseRead() {
	close(s.ReadChan)
}

func FromStepToDataset(step *Step, output *Dataset) {
	if output == nil {
		return
	}
	output.Step = step
	step.Output = output
}

func FromDatasetToStep(input *Dataset, step *Step) {
	if input == nil {
		return
	}
	step.Inputs = append(step.Inputs, input)
	input.ReadingSteps = append(input.ReadingSteps, step)
}

func FromDatasetShardToTask(shard *DatasetShard, task *Task) {
	shard.ReadingTasks = append(shard.ReadingTasks, task)
	task.Inputs = append(task.Inputs, shard)
}

func FromTaskToDatasetShard(task *Task, shard *DatasetShard) {
	if shard != nil {
		task.Outputs = append(task.Outputs, shard)
	}
}
