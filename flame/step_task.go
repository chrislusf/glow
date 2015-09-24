package flame

import (
	"reflect"
	"sync"
)

type Task struct {
	Id      int
	Inputs  []*DatasetShard
	Outputs []*DatasetShard
	Step    *Step
}

// source ->w:ds:r -> task -> w:ds:r
// source close next ds' w chan
// ds close its own r chan
// task closes its own channel to next ds' w:ds

func (t *Task) Run() {
	// println("run  step", t.Step.Id, "task", t.Id)
	t.Step.Function(t)
	for _, out := range t.Outputs {
		out.WriteChan.Close()
	}
	// println("stop step", t.Step.Id, "task", t.Id)
}

func (t *Task) InputChan() chan reflect.Value {
	if len(t.Inputs) == 1 {
		return t.Inputs[0].ReadChan
	}
	var prevChans []chan reflect.Value
	for _, c := range t.Inputs {
		prevChans = append(prevChans, c.ReadChan)
	}
	return merge(prevChans)
}

func merge(cs []chan reflect.Value) (out chan reflect.Value) {
	var wg sync.WaitGroup

	out = make(chan reflect.Value)

	for _, c := range cs {
		wg.Add(1)
		go func(c chan reflect.Value) {
			defer wg.Done()
			for n := range c {
				out <- n
			}
		}(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return
}
