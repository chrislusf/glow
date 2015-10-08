package flow

import (
	"reflect"
)

// reference by task runner on remote mode
var Contexts []*FlowContext

type FlowContext struct {
	Id       int
	Steps    []*Step
	Datasets []*Dataset
}

func NewContext() (fc *FlowContext) {
	fc = &FlowContext{Id: len(Contexts)}
	Contexts = append(Contexts, fc)
	return
}

func (fc *FlowContext) newNextDataset(shardSize int, t reflect.Type) (ret *Dataset) {
	if t != nil {
		ret = NewDataset(fc, t)
		ret.SetupShard(shardSize)
	}
	return
}

// the tasks should run on the source dataset shard
func (f *FlowContext) AddOneToOneStep(input *Dataset, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps), Type: Local}
	if output != nil {
		output.Step = s
	}
	if input != nil {
		s.Inputs = append(s.Inputs, input)
		input.AddReadingStep(s)
	}
	// setup the network
	for i, shard := range input.GetShards() {
		t := &Task{Inputs: []*DatasetShard{shard}, Step: s, Id: i}
		if output != nil {
			t.Outputs = []*DatasetShard{output.GetShards()[i]}
		}
		shard.AddReadingTask(t)
		s.Tasks = append(s.Tasks, t)
	}
	f.Steps = append(f.Steps, s)
	return
}

// the task should run on the destination dataset shard
func (f *FlowContext) AddAllToOneStep(input *Dataset, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps), Type: Network}
	if output != nil {
		output.Step = s
	}
	s.Inputs = append(s.Inputs, input)
	if len(input.Shards) == 1 {
		s.Type = Local
	}
	// setup the network
	t := &Task{Step: s, Id: 0}
	if output != nil {
		t.Outputs = []*DatasetShard{output.GetShards()[0]}
	}
	for _, shard := range input.GetShards() {
		shard.AddReadingTask(t)
		t.Inputs = append(t.Inputs, shard)
	}
	s.Tasks = append(s.Tasks, t)
	input.AddReadingStep(s)
	f.Steps = append(f.Steps, s)
	return
}

// the task should run on the source dataset shard
// input is nil for initial source dataset
func (f *FlowContext) AddOneToAllStep(input *Dataset, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps), Type: Local}
	if output != nil {
		output.Step = s
	}
	if input != nil {
		s.Inputs = append(s.Inputs, input)
		input.AddReadingStep(s)
	}
	// setup the network
	t := &Task{Step: s, Id: 0}
	if input != nil {
		shard := input.GetShards()[0]
		shard.AddReadingTask(t)
		t.Inputs = []*DatasetShard{shard}
	}
	for _, shard := range output.GetShards() {
		t.Outputs = append(t.Outputs, shard)
	}
	s.Tasks = append(s.Tasks, t)
	f.Steps = append(f.Steps, s)
	return
}

func (f *FlowContext) AddOneToEveryNStep(input *Dataset, n int, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps), Type: Local}
	if output != nil {
		output.Step = s
	}
	if input != nil {
		s.Inputs = append(s.Inputs, input)
		input.AddReadingStep(s)
	}
	// setup the network
	m := len(input.GetShards())
	for i, inShard := range input.GetShards() {
		t := &Task{Inputs: []*DatasetShard{inShard}, Step: s, Id: len(s.Tasks)}
		for k := 0; k < n; k++ {
			t.Outputs = append(t.Outputs, output.GetShards()[k*m+i])
		}
		inShard.AddReadingTask(t)
		s.Tasks = append(s.Tasks, t)
	}
	f.Steps = append(f.Steps, s)
	return
}

func (f *FlowContext) AddEveryNToOneStep(input *Dataset, m int, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps), Type: Network}
	if output != nil {
		output.Step = s
	}
	if input != nil {
		s.Inputs = append(s.Inputs, input)
		input.AddReadingStep(s)
	}
	if m == 1 {
		s.Type = Local
	}
	// setup the network
	n := len(output.GetShards())
	for i, outShard := range output.GetShards() {
		t := &Task{Outputs: []*DatasetShard{outShard}, Step: s, Id: len(s.Tasks)}
		for k := 0; k < m; k++ {
			inShard := input.GetShards()[k*n+i]
			inShard.AddReadingTask(t)
			t.Inputs = append(t.Inputs, inShard)
		}
		s.Tasks = append(s.Tasks, t)
	}
	f.Steps = append(f.Steps, s)
	return
}

// All dataset should have the same number of shards.
func (f *FlowContext) MergeDatasets1ShardTo1Step(inputs []*Dataset, output *Dataset) (s *Step) {
	s = &Step{Output: output, Id: len(f.Steps), Type: Network}
	if output != nil {
		output.Step = s
	}
	for _, input := range inputs {
		if input != nil {
			s.Inputs = append(s.Inputs, input)
		}
		input.AddReadingStep(s)
	}
	// setup the network
	if output != nil {
		for shardId, outShard := range output.Shards {
			t := &Task{Step: s, Id: shardId}
			for _, input := range inputs {
				inShard := input.GetShards()[shardId]
				inShard.AddReadingTask(t)
				t.Inputs = append(t.Inputs, inShard)
			}
			if output != nil {
				t.Outputs = append(t.Outputs, outShard)
			}
			s.Tasks = append(s.Tasks, t)
		}
	}
	f.Steps = append(f.Steps, s)
	return
}
