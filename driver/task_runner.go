package driver

import (
	"bytes"
	"encoding/gob"
	"flag"
	"log"
	"reflect"
	"sync"

	"github.com/chrislusf/glow/driver/scheduler"
	"github.com/chrislusf/glow/flow"
)

type TaskOption struct {
	ContextId   int
	TaskGroupId int
}

var taskOption TaskOption

func init() {
	flag.IntVar(&taskOption.ContextId, "task.context.id", -1, "context id")
	flag.IntVar(&taskOption.TaskGroupId, "task.taskGroup.id", -1, "task group id")

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

	taskGroups := scheduler.GroupTasks(fc)

	tr.Tasks = taskGroups[tr.option.TaskGroupId].Tasks

	if len(tr.Tasks) == 0 {
		log.Println("How can the task group has no tasks!")
		return
	}

	// 4. setup task input and output channels
	var wg sync.WaitGroup
	tr.connectInputsAndOutputs(&wg)
	// 6. starts to run the task locally
	for _, task := range tr.Tasks {
		wg.Add(1)
		go func(task *flow.Task) {
			defer wg.Done()
			task.Run()
		}(task)
	}
	// 7. need to close connected output channels
	wg.Wait()
}

func (tr *TaskRunner) connectInputsAndOutputs(wg *sync.WaitGroup) {
	tr.connectExternalInputs(wg)
	tr.connectInternalInputsAndOutputs(wg)
	tr.connectExternalOutputs(wg)
}

func (tr *TaskRunner) connectInternalInputsAndOutputs(wg *sync.WaitGroup) {
	for i, _ := range tr.Tasks {
		if i == len(tr.Tasks)-1 {
			continue
		}
		currentShard, nextShard := tr.Tasks[i].Outputs[0], tr.Tasks[i+1].Inputs[0]
		wg.Add(1)
		go func(currentShard, nextShard *flow.DatasetShard, i int) {
			defer wg.Done()
			for {
				if t, ok := currentShard.WriteChan.Recv(); ok {
					nextShard.ReadChan <- t
				} else {
					close(nextShard.ReadChan)
					break
				}
			}
		}(currentShard, nextShard, i)
	}
}

func (tr *TaskRunner) connectExternalInputs(wg *sync.WaitGroup) {
	task := tr.Tasks[0]
	for _, shard := range task.Inputs {
		d := shard.Parent
		readChanName := shard.Name()
		// println("taskGroup", tr.option.TaskGroupId, "step", task.Step.Id, "task", task.Id, "trying to read from:", readChanName)
		rawChan, err := GetReadChannel(readChanName)
		if err != nil {
			log.Panic(err)
		}
		shard.ReadChan = rawReadChannelToTyped(rawChan, d.Type, wg)
	}
}

func (tr *TaskRunner) connectExternalOutputs(wg *sync.WaitGroup) {
	task := tr.Tasks[len(tr.Tasks)-1]
	for _, shard := range task.Outputs {
		writeChanName := shard.Name()
		// println("taskGroup", tr.option.TaskGroupId, "step", task.Step.Id, "task", task.Id, "writing to:", writeChanName)
		rawChan, err := GetSendChannel(writeChanName, wg)
		if err != nil {
			log.Panic(err)
		}
		connectTypedWriteChannelToRaw(shard.WriteChan, rawChan, wg)
	}
}

func rawReadChannelToTyped(c chan []byte, t reflect.Type, wg *sync.WaitGroup) chan reflect.Value {

	out := make(chan reflect.Value)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for data := range c {
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			v := reflect.New(t)
			if err := dec.DecodeValue(v); err != nil {
				log.Fatal("data type:", v.Kind(), " decode error:", err)
			} else {
				out <- reflect.Indirect(v)
			}
		}

		close(out)
	}()

	return out

}

func connectTypedWriteChannelToRaw(writeChan reflect.Value, c chan []byte, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		var t reflect.Value
		for ok := true; ok; {
			if t, ok = writeChan.Recv(); ok {
				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				if err := enc.EncodeValue(t); err != nil {
					log.Fatal("data type:", t.Type().String(), " ", t.Kind(), " encode error:", err)
				}
				c <- buf.Bytes()
			}
		}
		close(c)

	}()

}
