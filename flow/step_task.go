package flow

import (
	"bytes"
	"log"
	"reflect"
	"strconv"

	"github.com/chrislusf/glow/util"
)

type Task struct {
	Id         int
	Inputs     []*DatasetShard
	Outputs    []*DatasetShard
	Step       *Step
	InputChans []chan reflect.Value
}

func (step *Step) NewTask() (task *Task) {
	task = &Task{Step: step, Id: len(step.Tasks)}
	step.Tasks = append(step.Tasks, task)
	return
}

// source ->w:ds:r -> task -> w:ds:r
// source close next ds' w chan
// ds close its own r chan
// task closes its own channel to next ds' w:ds

func (t *Task) RunTask() {
	// println("start", t.Name())
	t.Step.Function(t)
	for _, out := range t.Outputs {
		// println(t.Name(), "close WriteChan of", out.Name())
		out.WriteChan.Close()
	}
	// println("stop", t.Name())
}

func (t *Task) Name() string {
	var buffer bytes.Buffer
	if t.Step.Name != "" {
		buffer.WriteString(t.Step.Name)
	} else {
		buffer.WriteString("t")
	}
	buffer.WriteString(strconv.Itoa(t.Step.Id))
	if len(t.Step.Tasks) > 1 {
		buffer.WriteString("_")
		buffer.WriteString(strconv.Itoa(t.Id))
		buffer.WriteString("_")
		buffer.WriteString(strconv.Itoa(len(t.Step.Tasks)))
	}
	return buffer.String()
}

func (t *Task) InputChan() chan reflect.Value {
	if len(t.InputChans) != 1 {
		log.Panicf("This should not happen! task %s input should be only one instead of %d", t.Name(), len(t.InputChans))
	}
	return t.InputChans[0]
}

func (t *Task) AddInputChan(ch chan reflect.Value) {
	t.InputChans = append(t.InputChans, ch)
}

func (t *Task) MergedInputChan() chan reflect.Value {
	if len(t.InputChans) == 1 {
		return t.InputChans[0]
	}
	var prevChans []chan reflect.Value
	for _, c := range t.InputChans {
		prevChans = append(prevChans, c)
	}
	return util.MergeChannel(prevChans)
}
