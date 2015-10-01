package scheduler

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/chrislusf/glow/driver/scheduler/market"
	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/resource"
)

type SubmitTaskGroup struct {
	FlowContext *flow.FlowContext
	TaskGroup   *TaskGroup
	Bid         int
	WaitGroup   *sync.WaitGroup
}

type ReleaseTaskGroupInputs struct {
	FlowContext *flow.FlowContext
	TaskGroup   *TaskGroup
	WaitGroup   *sync.WaitGroup
}

/*
resources are leased to driver, expires every X miniute unless renewed.
1. request resource
2. release resource
*/
func (s *Scheduler) EventLoop() {
	for {
		event := <-s.EventChan
		switch event := event.(type) {
		default:
		case SubmitTaskGroup:
			// fmt.Printf("processing %+v\n", event)
			taskGroup := event.TaskGroup
			pickedServerChan := make(chan market.Supply, 1)
			s.Market.AddDemand(market.Requirement(taskGroup), event.Bid, pickedServerChan)
			event.WaitGroup.Add(1)
			go func() {
				defer event.WaitGroup.Done()

				// get assigned executor
				supply := <-pickedServerChan
				allocation := supply.Object.(resource.Allocation)

				// remember dataset location
				tasks := event.TaskGroup.Tasks
				for _, ds := range tasks[len(tasks)-1].Outputs {
					fmt.Printf("remember %s -> %v\n", ds.Name(), allocation.Location)
					s.datasetShard2Location[ds.Name()] = allocation.Location
				}

				dir, _ := os.Getwd()
				request := NewStartRequest(os.Args[0], dir,
					"-task.context.id",
					strconv.Itoa(event.FlowContext.Id),
					"-task.taskGroup.id",
					strconv.Itoa(taskGroup.Id),
				)
				// fmt.Printf("starting on %s: %v\n", server, request)
				if err := RemoteDirectExecute(allocation.Location.URL(), request); err != nil {
					println("exeuction error:", err.Error())
				} else {
					s.Market.ReturnSupply(supply)
				}
			}()
		case ReleaseTaskGroupInputs:
			taskGroup := event.TaskGroup
			event.WaitGroup.Add(1)
			go func() {
				defer event.WaitGroup.Done()

				tasks := event.TaskGroup.Tasks
				for _, ds := range tasks[0].Inputs {
					fmt.Printf("delete %s -> %v\n", ds.Name(), allocation.Location)
					s.datasetShard2Location[ds.Name()] = allocation.Location
				}
			}()
		case *bool:
		case *int:
		}
	}
}
