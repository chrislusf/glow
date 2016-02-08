package scheduler

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/chrislusf/glow/driver/plan"
	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/netchan"
	"github.com/chrislusf/glow/resource"
)

func (s *Scheduler) remoteExecuteOnLocation(flowContext *flow.FlowContext, taskGroup *plan.TaskGroup, allocation resource.Allocation, wg *sync.WaitGroup) {
	tasks := taskGroup.Tasks

	s.setupInputChannels(flowContext, tasks[0], allocation.Location, wg)

	for _, shard := range tasks[len(tasks)-1].Outputs {
		s.shardLocator.SetShardLocation(s.option.ExecutableFileHash+"-"+shard.Name(), allocation.Location)
	}
	s.setupOutputChannels(tasks[len(tasks)-1].Outputs, wg)

	// fmt.Printf("allocated %s on %v\n", tasks[0].Name(), allocation.Location)
	// create reqeust
	args := []string{
		"-glow.flow.id",
		strconv.Itoa(flowContext.Id),
		"-glow.taskGroup.id",
		strconv.Itoa(taskGroup.Id),
		"-glow.task.name",
		tasks[0].Name(),
		"-glow.agent.port",
		strconv.Itoa(allocation.Location.Port),
		"-glow.taskGroup.inputs",
		s.shardLocator.allInputLocations(tasks[0]),
		"-glow.exe.hash",
		s.shardLocator.executableFileHash,
		"-glow.channel.bufferSize",
		strconv.Itoa(flowContext.ChannelBufferSize),
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

	requestId := request.StartRequest.GetHashCode()
	status, isOld := s.getRemoteExecutorStatus(requestId)
	if isOld {
		log.Printf("Replacing old request: %v", status)
	}
	status.RequestTime = time.Now()
	status.Allocation = allocation
	status.Request = request
	taskGroup.RequestId = requestId

	// fmt.Printf("starting on %s: %v\n", allocation.Allocated, request)
	if err := RemoteDirectExecute(allocation.Location.URL(), request); err != nil {
		log.Printf("exeuction error %v: %v", err, request)
	}
	status.StopTime = time.Now()
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
		inputChanName := fmt.Sprintf("%s-ct-%d-input-%d-p-%d", s.option.ExecutableFileHash, fc.Id, ds.Id, i)
		// println("setup input channel for", task.Name(), "on", location.URL())
		s.shardLocator.SetShardLocation(inputChanName, location)
		rawChan, err := netchan.GetDirectSendChannel(s.option.TlsConfig, inputChanName, location.URL(), waitGroup)
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
		readChanName := s.option.ExecutableFileHash + "-" + shard.Name()
		location, _ := s.shardLocator.GetShardLocation(readChanName)
		rawChan, err := netchan.GetDirectReadChannel(s.option.TlsConfig, readChanName, location.URL(), 1024)
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
