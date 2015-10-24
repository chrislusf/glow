package driver

import (
	"flag"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"

	"github.com/chrislusf/glow/driver/plan"
	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/io"
)

type TaskOption struct {
	ContextId    int
	TaskGroupId  int
	FistTaskName string
	Inputs       string
}

var taskOption TaskOption

func init() {
	flag.IntVar(&taskOption.ContextId, "glow.flow.id", -1, "flow id")
	flag.IntVar(&taskOption.TaskGroupId, "glow.taskGroup.id", -1, "task group id")
	flag.StringVar(&taskOption.FistTaskName, "glow.task.name", "", "name of first task in the task group")
	flag.StringVar(&taskOption.Inputs, "glow.taskGroup.inputs", "", "comma and @ seperated input locations")

	flow.RegisterTaskRunner(NewTaskRunner(&taskOption))
}

type TaskRunner struct {
	option *TaskOption
	Tasks  []*flow.Task
}

func NewTaskRunner(option *TaskOption) *TaskRunner {
	return &TaskRunner{option: option}
}

func (tr *TaskRunner) IsTaskMode() bool {
	return tr.option.TaskGroupId >= 0 && tr.option.ContextId >= 0
}

// if this should not run, return false
func (tr *TaskRunner) Run(fc *flow.FlowContext) {

	taskGroups := plan.GroupTasks(fc)

	tr.Tasks = taskGroups[tr.option.TaskGroupId].Tasks

	if len(tr.Tasks) == 0 {
		log.Println("How can the task group has no tasks!")
		return
	}

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
	task := tr.Tasks[0]
	for i, shard := range task.Inputs {
		d := shard.Parent
		readChanName := shard.Name()
		// println("taskGroup", tr.option.TaskGroupId, "task", task.Name(), "trying to read from:", readChanName, len(task.InputChans))
		rawChan, err := io.GetDirectReadChannel(readChanName, name2Location[readChanName])
		if err != nil {
			log.Panic(err)
		}
		io.ConnectRawReadChannelToTyped(rawChan, task.InputChans[i], d.Type, wg)
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
		inputChanName := fmt.Sprintf("ct-%d-input-%d-p-%d", tr.option.ContextId, ds.Id, i)
		rawChan, err := io.GetLocalReadChannel(inputChanName)
		if err != nil {
			log.Panic(err)
		}
		typedInputChan := make(chan reflect.Value)
		io.ConnectRawReadChannelToTyped(rawChan, typedInputChan, ds.Type, wg)
		firstTask.InputChans = append(firstTask.InputChans, typedInputChan)
	}
}

func (tr *TaskRunner) connectExternalOutputs(wg *sync.WaitGroup) {
	task := tr.Tasks[len(tr.Tasks)-1]
	for _, shard := range task.Outputs {
		writeChanName := shard.Name()
		// println("taskGroup", tr.option.TaskGroupId, "step", task.Step.Id, "task", task.Id, "writing to:", writeChanName)
		rawChan, err := io.GetLocalSendChannel(writeChanName, wg)
		if err != nil {
			log.Panic(err)
		}
		io.ConnectTypedWriteChannelToRaw(shard.WriteChan, rawChan, wg)
	}
}
