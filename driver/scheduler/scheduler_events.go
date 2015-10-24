package scheduler

import (
	"bytes"
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
	"github.com/chrislusf/glow/io"
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
				s.waitForInputDatasetShardLocations(tasks[0])
				// fmt.Printf("inputs of %s is %s\n", tasks[0].Name(), s.allInputLocations(tasks[0]))

				s.Market.AddDemand(market.Requirement(taskGroup), event.Bid, pickedServerChan)

				// get assigned executor location
				supply := <-pickedServerChan
				allocation := supply.Object.(resource.Allocation)
				defer s.Market.ReturnSupply(supply)

				s.setupInputChannels(event.FlowContext, tasks[0], allocation.Location, event.WaitGroup)

				for _, shard := range tasks[len(tasks)-1].Outputs {
					s.registerDatasetShardLocation(shard.Name(), allocation.Location)
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
					s.allInputLocations(tasks[0]),
				}
				for _, arg := range os.Args[1:] {
					args = append(args, arg)
				}
				request := NewStartRequest(
					"./"+filepath.Base(os.Args[0]),
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
			// fmt.Printf("%s's input %s is not ready\n", task.Name(), input.Name())
			return false
		}
	}
	return true
}

func (s *Scheduler) waitForInputDatasetShardLocations(task *flow.Task) {
	s.datasetShard2LocationLock.Lock()
	defer s.datasetShard2LocationLock.Unlock()

	for !s.allInputsAreRegistered(task) {
		s.waitForAllInputs.Wait()
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
		s.registerDatasetShardLocation(inputChanName, location)
		rawChan, err := io.GetDirectSendChannel(inputChanName, location.URL(), waitGroup)
		if err != nil {
			log.Panic(err)
		}
		// println("writing", inputChanName, "to", location.URL())
		io.ConnectTypedWriteChannelToRaw(inChan, rawChan, waitGroup)
	}
}

func (s *Scheduler) registerDatasetShardLocation(name string, location resource.Location) {
	s.datasetShard2LocationLock.Lock()
	defer s.datasetShard2LocationLock.Unlock()

	// fmt.Printf("shard %s is at %s\n", name, location.URL())
	s.datasetShard2Location[name] = location
	s.waitForAllInputs.Broadcast()
}

func (s *Scheduler) setupOutputChannels(shards []*flow.DatasetShard, waitGroup *sync.WaitGroup) {
	for _, shard := range shards {
		ds := shard.Parent
		if len(ds.ExternalOutputChans) == 0 {
			continue
		}
		// connect remote raw chan to local typed chan
		readChanName := shard.Name()
		location := s.datasetShard2Location[readChanName]
		rawChan, err := io.GetDirectReadChannel(readChanName, location.URL())
		if err != nil {
			log.Panic(err)
		}
		for _, out := range ds.ExternalOutputChans {
			ch := make(chan reflect.Value)
			io.ConnectRawReadChannelToTyped(rawChan, ch, ds.Type, waitGroup)
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				for v := range ch {
					v = io.CleanObject(v, ds.Type, out.Type().Elem())
					out.Send(v)
				}
			}()
		}
	}
}

func (s *Scheduler) allInputLocations(task *flow.Task) string {
	s.datasetShard2LocationLock.Lock()
	defer s.datasetShard2LocationLock.Unlock()

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
