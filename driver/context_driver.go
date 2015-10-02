package driver

import (
	"flag"
	"os"
	"sync"

	"github.com/chrislusf/glow/driver/rsync"
	"github.com/chrislusf/glow/driver/scheduler"
	"github.com/chrislusf/glow/flow"
)

type DriverOption struct {
	ShouldStart bool
	Leader      string
	DataCenter  string
	Rack        string
}

func init() {
	var driverOption DriverOption
	flag.BoolVar(&driverOption.ShouldStart, "driver", false, "start in driver mode")
	flag.StringVar(&driverOption.Leader, "driver.leader", "localhost:8930", "leader server")
	flag.StringVar(&driverOption.DataCenter, "driver.dataCenter", "defaultDataCenter", "preferred data center name")
	flag.StringVar(&driverOption.Rack, "driver.rack", "defaultRack", "preferred rack name")

	flow.RegisterContextRunner(NewFlowContextDriver(&driverOption))
}

type FlowContextDriver struct {
	option *DriverOption
}

func NewFlowContextDriver(option *DriverOption) *FlowContextDriver {
	return &FlowContextDriver{option: option}
}

func (fcd *FlowContextDriver) IsDriverMode() bool {
	return fcd.option.ShouldStart
}

// driver runs on local, controlling all tasks
func (fcd *FlowContextDriver) Run(fc *flow.FlowContext) {

	// rsyncServer :=
	rsync.NewRsyncServer(os.Args[0])
	// rsyncServer.Start()

	taskGroups := scheduler.GroupTasks(fc)

	sched := scheduler.NewScheduler(
		fcd.option.Leader,
		&scheduler.SchedulerOption{
			DataCenter: fcd.option.DataCenter,
			Rack:       fcd.option.Rack,
		},
	)
	go sched.EventLoop()

	// schedule to run the steps
	var wg sync.WaitGroup
	for i, taskGroup := range taskGroups {
		wg.Add(1)
		sched.EventChan <- scheduler.SubmitTaskGroup{
			FlowContext: fc,
			TaskGroup:   taskGroup,
			Bid:         len(taskGroups) - i,
			WaitGroup:   &wg,
		}
	}
	go sched.Market.FetcherLoop()

	wg.Wait()

	wg.Add(1)
	sched.EventChan <- scheduler.ReleaseTaskGroupInputs{
		FlowContext: fc,
		TaskGroups:  taskGroups,
		WaitGroup:   &wg,
	}

	wg.Wait()
}
