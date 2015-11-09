package scheduler

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"

	"github.com/chrislusf/glow/driver/plan"
	"github.com/chrislusf/glow/driver/scheduler/market"
	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/netchan"
	"github.com/chrislusf/glow/resource"
)

type SubmitTaskGroup struct {
	FlowContext *flow.FlowContext
	TaskGroup   *plan.TaskGroup
	Bid         float64
	WaitGroup   *sync.WaitGroup
}

type ReleaseTaskGroupInputs struct {
	FlowContext *flow.FlowContext
	TaskGroups  []*plan.TaskGroup
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
			go func() {
				defer event.WaitGroup.Done()
				tasks := event.TaskGroup.Tasks

				// wait until inputs are registed
				s.shardLocator.waitForInputDatasetShardLocations(tasks[0])
				// fmt.Printf("inputs of %s is %s\n", tasks[0].Name(), s.allInputLocations(tasks[0]))

				s.Market.AddDemand(market.Requirement(taskGroup), event.Bid, pickedServerChan)

				// get assigned executor location
				supply := <-pickedServerChan
				allocation := supply.Object.(resource.Allocation)
				defer s.Market.ReturnSupply(supply)

				s.setupInputChannels(event.FlowContext, tasks[0], allocation.Location, event.WaitGroup)

				for _, shard := range tasks[len(tasks)-1].Outputs {
					s.shardLocator.SetShardLocation(shard.Name(), allocation.Location)
				}
				s.setupOutputChannels(tasks[len(tasks)-1].Outputs, event.WaitGroup)

				// fmt.Printf("allocated %s on %v\n", tasks[0].Name(), allocation.Location)
				// create reqeust
				args := []string{
					"-glow.flow.id",
					strconv.Itoa(event.FlowContext.Id),
					"-glow.taskGroup.id",
					strconv.Itoa(taskGroup.Id),
					"-glow.task.name",
					tasks[0].Name(),
					"-glow.agent.port",
					strconv.Itoa(allocation.Location.Port),
					"-glow.taskGroup.inputs",
					s.shardLocator.allInputLocations(tasks[0]),
				}
				for _, arg := range os.Args[1:] {
					args = append(args, arg)
				}
				request := NewStartRequest(
					"./"+filepath.Base(os.Args[0]),
					// filepath.Join(".", filepath.Base(os.Args[0])),
					s.option.Module,
					args,
					allocation.Allocated,
					os.Environ(),
					int32(s.option.DriverPort),
				)

				// fmt.Printf("starting on %s: %v\n", allocation.Allocated, request)
				if err := RemoteDirectExecute(allocation.Location.URL(), request); err != nil {
					log.Printf("exeuction error %v: %v", err, request)
				}
			}()
		case ReleaseTaskGroupInputs:
			go func() {
				defer event.WaitGroup.Done()

				for _, taskGroup := range event.TaskGroups {
					tasks := taskGroup.Tasks
					for _, ds := range tasks[len(tasks)-1].Outputs {
						location, _ := s.shardLocator.GetShardLocation(ds.Name())
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

func (s *Scheduler) setupInputChannels(fc *flow.FlowContext, task *flow.Task, location resource.Location, waitGroup *sync.WaitGroup) {
	if len(task.Inputs) > 0 {
		return
	}
	ds := task.Outputs[0].Parent
	if len(ds.ExternalInputChans) == 0 {
		return
	}
	// connect local typed chan to remote raw chan
	// write to the dataset location in the cluster so that the task can be retried if needed.
	for i, inChan := range ds.ExternalInputChans {
		inputChanName := fmt.Sprintf("ct-%d-input-%d-p-%d", fc.Id, ds.Id, i)
		// println("setup input channel for", task.Name(), "on", location.URL())
		s.shardLocator.SetShardLocation(inputChanName, location)
		rawChan, err := netchan.GetDirectSendChannel(inputChanName, location.URL(), waitGroup)
		if err != nil {
			log.Panic(err)
		}
		// println("writing", inputChanName, "to", location.URL())
		netchan.ConnectTypedWriteChannelToRaw(inChan, rawChan, waitGroup)
	}
}

func (s *Scheduler) setupOutputChannels(shards []*flow.DatasetShard, waitGroup *sync.WaitGroup) {
	for _, shard := range shards {
		ds := shard.Parent
		if len(ds.ExternalOutputChans) == 0 {
			continue
		}
		// connect remote raw chan to local typed chan
		readChanName := shard.Name()
		location, _ := s.shardLocator.GetShardLocation(readChanName)
		rawChan, err := netchan.GetDirectReadChannel(readChanName, location.URL(), 1024)
		if err != nil {
			log.Panic(err)
		}
		for _, out := range ds.ExternalOutputChans {
			ch := make(chan reflect.Value)
			netchan.ConnectRawReadChannelToTyped(rawChan, ch, ds.Type, waitGroup)
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				for v := range ch {
					v = netchan.CleanObject(v, ds.Type, out.Type().Elem())
					out.Send(v)
				}
			}()
		}
	}
}
