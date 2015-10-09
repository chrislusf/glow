package scheduler

import (
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
	TaskGroups  []*TaskGroup
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
			go func() {
				defer event.WaitGroup.Done()

				// get assigned executor
				supply := <-pickedServerChan
				allocation := supply.Object.(resource.Allocation)

				// remember dataset location
				tasks := event.TaskGroup.Tasks
				for _, ds := range tasks[len(tasks)-1].Outputs {
					name := ds.Name()
					location := allocation.Location
					s.datasetShard2LocationLock.Lock()
					s.datasetShard2Location[name] = location
					s.datasetShard2LocationLock.Unlock()
				}

				dir, _ := os.Getwd()
				args := []string{
					"-glow.context.id",
					strconv.Itoa(event.FlowContext.Id),
					"-glow.taskGroup.id",
					strconv.Itoa(taskGroup.Id),
					"-glow.task.name",
					tasks[0].Name(),
				}
				for _, arg := range os.Args[1:] {
					args = append(args, arg)
				}
				request := NewStartRequest(os.Args[0], dir, args)
				// fmt.Printf("starting on %s: %v\n", server, request)
				if err := RemoteDirectExecute(allocation.Location.URL(), request); err != nil {
					println("exeuction error:", err.Error())
				} else {
					s.Market.ReturnSupply(supply)
				}
			}()
		case ReleaseTaskGroupInputs:
			go func() {
				defer event.WaitGroup.Done()

				for _, taskGroup := range event.TaskGroups {
					tasks := taskGroup.Tasks
					for _, ds := range tasks[len(tasks)-1].Outputs {
						location := s.datasetShard2Location[ds.Name()]
						request := NewDeleteDatasetShardRequest(ds.Name())
						// println("deleting", ds.Name(), "on", location.URL())
						if err := RemoteDirectExecute(location.URL(), request); err != nil {
							println("exeuction error:", err.Error())
						}
					}
				}
			}()
		case *bool:
		case *int:
		}
	}
}
