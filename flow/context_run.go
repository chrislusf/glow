package flow

import (
	"os"
	"sync"
)

/*
There are 3 running mode:
1. as normal program
	If not in distributed mode, it should not be intercepted.
2. "-driver" mode to drive in distributed mode
	context runner will register
3. "-task.[context|taskGroup].id" mode to run task in distributed mode
*/

var contextRunner ContextRunner
var taskRunner TaskRunner

// Invoked by driver task runner
func RegisterContextRunner(r ContextRunner) {
	contextRunner = r
}
func RegisterTaskRunner(r TaskRunner) {
	taskRunner = r
}

type ContextRunner interface {
	Run(*FlowContext)
	IsDriverMode() bool
	IsDriverPlotMode() bool
	Plot(*FlowContext)
}

type TaskRunner interface {
	Run(fc *FlowContext)
	IsTaskMode() bool
}

func Ready() {
	if taskRunner.IsTaskMode() {
		for _, fc := range Contexts {
			fc.Run()
		}
		os.Exit(0)
	} else if contextRunner.IsDriverMode() {
		if contextRunner.IsDriverPlotMode() {
			for _, fc := range Contexts {
				contextRunner.Plot(fc)
			}
			os.Exit(0)
		}
	} else {
	}
}

func (fc *FlowContext) Run() {

	if taskRunner.IsTaskMode() {
		taskRunner.Run(fc)
	} else if contextRunner.IsDriverMode() {
		contextRunner.Run(fc)
	} else {
		fc.run_standalone()
	}
}

func (fc *FlowContext) run_standalone() {

	var wg sync.WaitGroup

	// start all task edges
	for i, step := range fc.Steps {
		if i == 0 {
			wg.Add(1)
			go func(step *Step) {
				defer wg.Done()
				// println("start dataset", step.Id)
				for _, input := range step.Inputs {
					if input != nil {
						input.RunSelf(step.Id)
					}
				}
			}(step)
		}
		wg.Add(1)
		go func(step *Step) {
			defer wg.Done()
			step.Run()
		}(step)
		wg.Add(1)
		go func(step *Step) {
			defer wg.Done()
			// println("start dataset", step.Id+1)
			if step.Output != nil {
				step.Output.RunSelf(step.Id + 1)
			}
		}(step)
	}
	wg.Wait()
}
