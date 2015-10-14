package scheduler

import (
	"bytes"
	// "fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"

	"github.com/chrislusf/glow/driver/scheduler/market"
	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/io"
	"github.com/chrislusf/glow/resource"
)

type SubmitTaskGroup struct {
	FlowContext *flow.FlowContext
	TaskGroup   *TaskGroup
	Bid         float64
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
	waitForAllInputs := sync.NewCond(&s.datasetShard2LocationLock)
	for {
		event := <-s.EventChan
		switch event := event.(type) {
		default:
		case SubmitTaskGroup:
			// fmt.Printf("processing %+v\n", event)
			taskGroup := event.TaskGroup
			pickedServerChan := make(chan market.Supply, 1)
			go func() {
				defer event.WaitGroup.Done()
				tasks := event.TaskGroup.Tasks

				// wait until inputs are registed
				s.datasetShard2LocationLock.Lock()
				for !s.allInputsAreRegistered(tasks[0]) {
					// fmt.Printf("inputs of %s is not ready\n", tasks[0].Name())
					waitForAllInputs.Wait()
				}
				s.datasetShard2LocationLock.Unlock()
				// fmt.Printf("inputs of %s is %s\n", tasks[0].Name(), s.allInputLocations(tasks[0]))

				s.Market.AddDemand(market.Requirement(taskGroup), event.Bid, pickedServerChan)

				// get assigned executor
				supply := <-pickedServerChan
				allocation := supply.Object.(resource.Allocation)

				// remember dataset location
				for _, dss := range tasks[len(tasks)-1].Outputs {
					name := dss.Name()
					location := allocation.Location
					s.datasetShard2LocationLock.Lock()
					s.datasetShard2Location[name] = location
					waitForAllInputs.Broadcast()
					s.datasetShard2LocationLock.Unlock()
				}

				// setup output channel
				for _, shard := range tasks[len(tasks)-1].Outputs {
					ds := shard.Parent
					if len(ds.OutputChans) == 0 {
						continue
					}
					// connect remote raw ran to local typed chan
					readChanName := shard.Name()
					location := s.datasetShard2Location[readChanName]
					rawChan, err := io.GetDirectReadChannel(readChanName, location.URL())
					if err != nil {
						log.Panic(err)
					}
					for _, out := range ds.OutputChans {
						ch := make(chan reflect.Value)
						io.ConnectRawReadChannelToTyped(rawChan, ch, ds.Type, event.WaitGroup)
						event.WaitGroup.Add(1)
						go func() {
							defer event.WaitGroup.Done()
							for v := range ch {
								v = io.CleanObject(v, ds.Type, out.Type().Elem())
								out.Send(v)
							}
						}()
					}
				}

				// fmt.Printf("allocated %s on %v\n", tasks[0].Name(), allocation.Location)
				// create reqeust
				dir, _ := os.Getwd()
				args := []string{
					"-glow.context.id",
					strconv.Itoa(event.FlowContext.Id),
					"-glow.taskGroup.id",
					strconv.Itoa(taskGroup.Id),
					"-glow.task.name",
					tasks[0].Name(),
					"-glow.agent.port",
					strconv.Itoa(allocation.Location.Port),
					"-glow.taskGroup.inputs",
					s.allInputLocations(tasks[0]),
				}
				for _, arg := range os.Args[1:] {
					args = append(args, arg)
				}
				request := NewStartRequest(os.Args[0], dir, args, allocation.Allocated)

				// fmt.Printf("starting on %s: %v\n", allocation.Allocated, request)
				if err := RemoteDirectExecute(allocation.Location.URL(), request); err != nil {
					log.Printf("exeuction error %v: %v", err, request)
				} else {
					// fmt.Printf("Closing and returning resources on %s: %v\n", allocation.Allocated, request)
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
		}
	}
}

func (s *Scheduler) allInputsAreRegistered(task *flow.Task) bool {
	for _, input := range task.Inputs {
		if _, hasValue := s.datasetShard2Location[input.Name()]; !hasValue {
			return false
		}
	}
	return true
}

func (s *Scheduler) allInputLocations(task *flow.Task) string {
	var buf bytes.Buffer
	for i, input := range task.Inputs {
		name := input.Name()
		location, hasValue := s.datasetShard2Location[name]
		if !hasValue {
			panic("hmmm, we just checked all inputs are registered!")
		}
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(name)
		buf.WriteString("@")
		buf.WriteString(location.URL())
	}
	return buf.String()
}
