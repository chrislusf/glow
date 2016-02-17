// Package flow contains data structure for computation.
// Mostly Dataset operations such as Map/Reduce/Join/Sort etc.
package flow

import (
	"reflect"
)

// reference by task runner on exectutors
var Contexts []*FlowContext

type FlowContext struct {
	Id                int
	Steps             []*Step
	Datasets          []*Dataset
	ChannelBufferSize int // configurable channel buffer size for reading
}

func New() (fc *FlowContext) {
	fc = &FlowContext{Id: len(Contexts)}
	Contexts = append(Contexts, fc)
	return
}

func (fc *FlowContext) newNextDataset(shardSize int, dType reflect.Type) (ret *Dataset) {
	ret = NewDataset(fc, dType)
	if dType != nil {
		ret.SetupShard(shardSize)
	}
	return
}

func (fc *FlowContext) NewStep() (step *Step) {
	step = &Step{Id: len(fc.Steps)}
	fc.Steps = append(fc.Steps, step)
	return
}

// the tasks should run on the source dataset shard
func (f *FlowContext) AddOneToOneStep(input *Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	FromStepToDataset(step, output)
	FromDatasetToStep(input, step)

	// setup the network
	if output != nil && len(output.ExternalInputChans) > 0 {
		task := step.NewTask()
		FromTaskToDatasetShard(task, output.GetShards()[0])
	} else {
		for i, shard := range input.GetShards() {
			task := step.NewTask()
			if output != nil && output.Shards != nil {
				FromTaskToDatasetShard(task, output.GetShards()[i])
			}
			FromDatasetShardToTask(shard, task)
		}
	}
	return
}

// the task should run on the destination dataset shard
func (f *FlowContext) AddAllToOneStep(input *Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	FromStepToDataset(step, output)
	FromDatasetToStep(input, step)

	// setup the network
	task := step.NewTask()
	if output != nil {
		FromTaskToDatasetShard(task, output.GetShards()[0])
	}
	for _, shard := range input.GetShards() {
		FromDatasetShardToTask(shard, task)
	}
	return
}

// the task should run on the source dataset shard
// input is nil for initial source dataset
func (f *FlowContext) AddOneToAllStep(input *Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	FromStepToDataset(step, output)
	FromDatasetToStep(input, step)

	// setup the network
	task := step.NewTask()
	if input != nil {
		FromDatasetShardToTask(input.GetShards()[0], task)
	}
	for _, shard := range output.GetShards() {
		FromTaskToDatasetShard(task, shard)
	}
	return
}

func (f *FlowContext) AddOneToEveryNStep(input *Dataset, n int, output *Dataset) (step *Step) {
	step = f.NewStep()
	FromStepToDataset(step, output)
	FromDatasetToStep(input, step)

	// setup the network
	m := len(input.GetShards())
	for i, inShard := range input.GetShards() {
		task := step.NewTask()
		for k := 0; k < n; k++ {
			FromTaskToDatasetShard(task, output.GetShards()[k*m+i])
		}
		FromDatasetShardToTask(inShard, task)
	}
	return
}

func (f *FlowContext) AddLinkedNToOneStep(input *Dataset, m int, output *Dataset) (step *Step) {
	step = f.NewStep()
	FromStepToDataset(step, output)
	FromDatasetToStep(input, step)

	// setup the network
	for i, outShard := range output.GetShards() {
		task := step.NewTask()
		FromTaskToDatasetShard(task, outShard)
		for k := 0; k < m; k++ {
			FromDatasetShardToTask(input.GetShards()[i*m+k], task)
		}
	}
	return
}

// All dataset should have the same number of shards.
func (f *FlowContext) MergeDatasets1ShardTo1Step(inputs []*Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	FromStepToDataset(step, output)
	for _, input := range inputs {
		FromDatasetToStep(input, step)
	}

	// setup the network
	if output != nil {
		for shardId, outShard := range output.Shards {
			task := step.NewTask()
			for _, input := range inputs {
				FromDatasetShardToTask(input.GetShards()[shardId], task)
			}
			FromTaskToDatasetShard(task, outShard)
		}
	}
	return
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
