package flow

import (
	"sync"
)

type Step struct {
	Id       int
	Inputs   []*Dataset
	Output   *Dataset
	Function func(*Task)
	Tasks    []*Task
	Name     string
}

func (s *Step) RunStep() {
	var wg sync.WaitGroup
	for i, t := range s.Tasks {
		wg.Add(1)
		go func(i int, t *Task) {
			defer wg.Done()
			t.RunTask()
		}(i, t)
	}
	wg.Wait()

	return
}
