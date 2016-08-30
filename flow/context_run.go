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
	if taskRunner != nil && taskRunner.IsTaskMode() {
		for _, fc := range Contexts {
			fc.Run()
		}
		os.Exit(0)
	} else if contextRunner != nil && contextRunner.IsDriverMode() {
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

	if taskRunner != nil && taskRunner.IsTaskMode() {
		taskRunner.Run(fc)
	} else if contextRunner != nil && contextRunner.IsDriverMode() {
		contextRunner.Run(fc)
	} else {
		fc.runFlowContextInStandAloneMode()
	}
}

func (fc *FlowContext) runFlowContextInStandAloneMode() {

	var wg sync.WaitGroup

	isDatasetStarted := make(map[int]bool)

	OnInterrupt(fc.OnInterrupt, nil)

	// start all task edges
	for _, step := range fc.Steps {
		for _, input := range step.Inputs {
			if _, ok := isDatasetStarted[input.Id]; !ok {
				wg.Add(1)
				go func(input *Dataset) {
					defer wg.Done()
					input.RunDatasetInStandAloneMode()
				}(input)
				isDatasetStarted[input.Id] = true
			}
		}
		wg.Add(1)
		go func(step *Step) {
			defer wg.Done()
			step.RunStep()
		}(step)

		if step.Output != nil {
			if _, ok := isDatasetStarted[step.Output.Id]; !ok {
				wg.Add(1)
				go func(step *Step) {
					defer wg.Done()
					step.Output.RunDatasetInStandAloneMode()
				}(step)
				isDatasetStarted[step.Output.Id] = true
			}
		}
	}
	wg.Wait()
}
