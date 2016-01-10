package driver

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"

	"github.com/chrislusf/glow/driver/plan"
	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/netchan"
)

type TaskRunner struct {
	option      *TaskOption
	Tasks       []*flow.Task
	FlowContext *flow.FlowContext
}

func NewTaskRunner(option *TaskOption) *TaskRunner {
	return &TaskRunner{option: option}
}

func (tr *TaskRunner) IsTaskMode() bool {
	return tr.option.TaskGroupId >= 0 && tr.option.ContextId >= 0
}

// if this should not run, return false
func (tr *TaskRunner) Run(fc *flow.FlowContext) {
	if fc.Id != tr.option.ContextId {
		return
	}
	fc.ChannelBufferSize = tr.option.ChannelBufferSize

	tr.Tasks = plan.GroupTasks(fc)[tr.option.TaskGroupId].Tasks
	tr.FlowContext = fc

	// println("taskGroup", tr.Tasks[0].Name(), "starts")
	// 4. setup task input and output channels
	var wg sync.WaitGroup
	tr.connectInputsAndOutputs(&wg)
	// 6. starts to run the task locally
	for _, task := range tr.Tasks {
		// println("run task", task.Name())
		wg.Add(1)
		go func(task *flow.Task) {
			defer wg.Done()
			task.RunTask()
		}(task)
	}
	// 7. need to close connected output channels
	wg.Wait()
	// println("taskGroup", tr.Tasks[0].Name(), "finishes")
}

func (tr *TaskRunner) connectInputsAndOutputs(wg *sync.WaitGroup) {
	name2Location := make(map[string]string)
	if tr.option.Inputs != "" {
		for _, nameLocation := range strings.Split(tr.option.Inputs, ",") {
			// println("input:", nameLocation)
			nl := strings.Split(nameLocation, "@")
			name2Location[nl[0]] = nl[1]
		}
	}
	tr.connectExternalInputChannels(wg)
	tr.connectExternalInputs(wg, name2Location)
	tr.connectInternalInputsAndOutputs(wg)
	tr.connectExternalOutputs(wg)
}

func (tr *TaskRunner) connectInternalInputsAndOutputs(wg *sync.WaitGroup) {
	for i, _ := range tr.Tasks {
		if i == len(tr.Tasks)-1 {
			continue
		}
		currentShard, nextShard := tr.Tasks[i].Outputs[0], tr.Tasks[i+1].Inputs[0]

		currentShard.SetupReadingChans()

		wg.Add(1)
		go func(currentShard, nextShard *flow.DatasetShard, i int) {
			defer wg.Done()
			for {
				if t, ok := currentShard.WriteChan.Recv(); ok {
					nextShard.SendForRead(t)
				} else {
					nextShard.CloseRead()
					break
				}
			}
		}(currentShard, nextShard, i)
	}
}

func (tr *TaskRunner) connectExternalInputs(wg *sync.WaitGroup, name2Location map[string]string) {
	firstTask := tr.Tasks[0]
	for i, shard := range firstTask.Inputs {
		d := shard.Parent
		readChanName := tr.option.ExecutableFileHash + "-" + shard.Name()
		// println("taskGroup", tr.option.TaskGroupId, "firstTask", firstTask.Name(), "trying to read from:", readChanName, len(firstTask.InputChans))
		rawChan, err := netchan.GetDirectReadChannel(readChanName, name2Location[readChanName], tr.FlowContext.ChannelBufferSize)
		if err != nil {
			log.Panic(err)
		}
		netchan.ConnectRawReadChannelToTyped(rawChan, firstTask.InputChans[i], d.Type, wg)
	}
}

func (tr *TaskRunner) connectExternalInputChannels(wg *sync.WaitGroup) {
	// this is only for Channel dataset
	firstTask := tr.Tasks[0]
	if firstTask.Inputs != nil {
		return
	}
	ds := firstTask.Outputs[0].Parent
	for i, _ := range ds.ExternalInputChans {
		inputChanName := fmt.Sprintf("%s-ct-%d-input-%d-p-%d", tr.option.ExecutableFileHash, tr.option.ContextId, ds.Id, i)
		rawChan, err := netchan.GetLocalReadChannel(inputChanName, tr.FlowContext.ChannelBufferSize)
		if err != nil {
			log.Panic(err)
		}
		typedInputChan := make(chan reflect.Value)
		netchan.ConnectRawReadChannelToTyped(rawChan, typedInputChan, ds.Type, wg)
		firstTask.InputChans = append(firstTask.InputChans, typedInputChan)
	}
}

func (tr *TaskRunner) connectExternalOutputs(wg *sync.WaitGroup) {
	lastTask := tr.Tasks[len(tr.Tasks)-1]
	for _, shard := range lastTask.Outputs {
		writeChanName := tr.option.ExecutableFileHash + "-" + shard.Name()
		// println("taskGroup", tr.option.TaskGroupId, "step", lastTask.Step.Id, "lastTask", lastTask.Id, "writing to:", writeChanName)
		rawChan, err := netchan.GetLocalSendChannel(writeChanName, wg)
		if err != nil {
			log.Panic(err)
		}
		netchan.ConnectTypedWriteChannelToRaw(shard.WriteChan, rawChan, wg)
	}
}
