package driver

import (
	"fmt"
	"sync"
	"time"

	"github.com/chrislusf/glow/driver/plan"
	"github.com/chrislusf/glow/driver/scheduler"
	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/util"
)

func (fcd *FlowContextDriver) OnInterrupt(
	fc *flow.FlowContext,
	sched *scheduler.Scheduler) {
	status := fcd.collectStatusFromRemoteExecutors(sched)
	fcd.printDistributedStatus(sched, status)
}

func (fcd *FlowContextDriver) OnExit(
	fc *flow.FlowContext,
	sched *scheduler.Scheduler) {
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
			// println("checking", request.Allocation.Location.URL(), requestId)
			if err := askExecutorToStopRequest(request.Allocation.Location.URL(), requestId); err != nil {
				fmt.Printf("Error to stop request %d on %s: %v\n", request.Allocation.Location.URL(), requestId, err)
				return
			}
		}(tg)
	}
	wg.Wait()

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
			// lastTask := tg.Tasks[len(tg.Tasks)-1]
			step := firstTask.Step
			if stat == nil {
				fmt.Printf("  No status.\n")
				continue
			}
			if stat.IsClosed() {
				fmt.Printf("  %s taskId:%d time:%v\n", stat.Allocation.Location.URL(), firstTask.Id, stat.TimeTaken())
			} else {
				fmt.Printf("  %s taskId:%d time:%v\n", stat.Allocation.Location.URL(), firstTask.Id, stat.TimeTaken())
			}
			if !stat.IsClosed() {
				for _, inputStat := range stat.InputChannelStatuses {
					fmt.Printf("    input  : d%d_%d  %d\n", step.Id, firstTask.Id, inputStat.Length)
				}
			}
			for _, outputStat := range stat.OutputChannelStatuses {
				fmt.Printf("    output : d%d_%d  %d\n", step.Id, firstTask.Id, outputStat.Length)
			}
		}

	}
	fmt.Print("\n")
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
			// println("checking", request.Allocation.Location.URL(), requestId)
			stat, err := askExecutorStatusForRequest(request.Allocation.Location.URL(), requestId)
			if err != nil {
				fmt.Printf("Error to request status from %s: %v\n", request.Allocation.Location.URL(), err)
				return
			}
			// println("back from", request.Allocation.Location.URL(), requestId)
			stat.Allocation = request.Allocation
			stat.taskGroup = tg
			stats[tg.Id] = stat
		}(tg)
	}
	wg.Wait()
	return stats
}

func askExecutorStatusForRequest(server string, requestId uint32) (*RemoteExecutorStatus, error) {

	reply, err := scheduler.RemoteDirectCommand(server, scheduler.NewGetStatusRequest(requestId))
	if err != nil {
		return nil, err
	}

	response := reply.GetGetStatusResponse()

	return &RemoteExecutorStatus{
		ExecutorStatus: util.ExecutorStatus{
			InputChannelStatuses:  FromProto(response.GetInputStatuses()),
			OutputChannelStatuses: FromProto(response.GetOutputStatuses()),
			RequestTime:           time.Unix(response.GetRequestTime(), 0),
			StartTime:             time.Unix(response.GetStartTime(), 0),
			StopTime:              time.Unix(response.GetStopTime(), 0),
		},
	}, nil
}

func askExecutorToStopRequest(server string, requestId uint32) (err error) {
	_, err = scheduler.RemoteDirectCommand(server, scheduler.NewStopRequest(requestId))
	return
}
