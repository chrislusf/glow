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
	WriteChan    reflect.Value
	ReadingTasks []*Task

	readingChans []chan reflect.Value
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

func (s *DatasetShard) Name() string {
	return fmt.Sprintf("ct-%d-ds-%d-shard-%d", s.Parent.context.Id, s.Parent.Id, s.Id)
}

func (shard *DatasetShard) SetupReadingChans() {
	for _, task := range shard.ReadingTasks {
		for i, s := range task.Inputs {
			if s == shard {
				shard.readingChans = append(shard.readingChans, task.InputChans[i])
			}
		}
	}
}

func (s *DatasetShard) SendForRead(t reflect.Value) {
	for _, c := range s.readingChans {
		c <- t
	}
}

func (s *DatasetShard) CloseRead() {
	for _, c := range s.readingChans {
		close(c)
	}
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
	task.InputChans = append(task.InputChans, make(chan reflect.Value))
}

func FromTaskToDatasetShard(task *Task, shard *DatasetShard) {
	if shard != nil {
		task.Outputs = append(task.Outputs, shard)
	}
}
