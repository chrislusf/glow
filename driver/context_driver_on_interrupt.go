package driver

import (
	"fmt"
	"sync"
	"time"

	"github.com/chrislusf/glow/driver/plan"
	"github.com/chrislusf/glow/driver/scheduler"
	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/resource"
	"github.com/chrislusf/glow/util"
)

func (fcd *FlowContextDriver) OnInterrupt(
	fc *flow.FlowContext,
	sched *scheduler.Scheduler) {
	status := fcd.collectStatusFromRemoteExecutors(sched)
	fcd.printDistributedStatus(sched, status)
}

func (fcd *FlowContextDriver) printDistributedStatus(sched *scheduler.Scheduler, stats []*RemoteExecutorStatus) {
	fmt.Print("\n")
	for _, stepGroup := range fcd.stepGroups {
		fmt.Print("step:")
		for _, step := range stepGroup.Steps {
			fmt.Printf(" %s%d", step.Name, step.Id)
		}
		fmt.Print("\n")

		for _, tg := range stepGroup.TaskGroups {
			stat := stats[tg.Id]
			firstTask := tg.Tasks[0]
			if stat.Closed() {
				fmt.Printf("  %s taskId:%d time:%v completed %d\n", stat.Allocation.Location.URL(), firstTask.Id, stat.TimeTaken(), 0)
			} else {
				fmt.Printf("  %s taskId:%d time:%v processed %d\n", stat.Allocation.Location.URL(), firstTask.Id, stat.TimeTaken(), 0)
			}
		}

	}
	fmt.Print("\n")
}

type RemoteExecutorStatus struct {
	ExecutorStatus
	Allocation resource.Allocation
	taskGroup  *plan.TaskGroup
}

func (fcd *FlowContextDriver) collectStatusFromRemoteExecutors(sched *scheduler.Scheduler) []*RemoteExecutorStatus {
	stats := make([]*RemoteExecutorStatus, len(fcd.taskGroups))
	var wg sync.WaitGroup
	for _, tg := range fcd.taskGroups {
		wg.Add(1)
		go func(tg *plan.TaskGroup) {
			defer wg.Done()

			requestId := tg.RequestId
			request, ok := sched.RemoteExecutorStatuses[requestId]
			if !ok {
				fmt.Printf("No executors for %v\n", tg)
				return
			}
			stat, err := askExecutorStatusForRequest(request.Allocation.Location.URL(), requestId)
			if err != nil {
				fmt.Printf("Error to request status from %s: %v\n", request.Allocation.Location.URL(), err)
				return
			}
			stat.Allocation = request.Allocation
			stat.taskGroup = tg
			stats[tg.Id] = stat
		}(tg)
	}
	wg.Wait()
	return stats
}

func askExecutorStatusForRequest(server string, requestId int32) (*RemoteExecutorStatus, error) {

	reply, err := scheduler.RemoteDirectCommand(server, scheduler.NewGetStatusRequest(requestId))
	if err != nil {
		return nil, err
	}

	response := reply.GetGetStatusResponse()

	var inputStatuses []*util.ChannelStatus
	for _, inputStatus := range response.GetInputStatuses() {

	}

	return &RemoteExecutorStatus{
		ExecutorStatus: ExecutorStatus{
			InputChannelStatuses: response.GetInputLength(),
			OutputChannelStatus: &util.ChannelStatus{
				Length: response.GetOutputLength(),
			},
			ReadyTime: time.Unix(response.GetReadyTime(), 0),
			StartTime: time.Unix(response.GetStartTime(), 0),
			StopTime:  time.Unix(response.GetStopTime(), 0),
		},
	}, nil
}
