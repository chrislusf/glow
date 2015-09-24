package driver

import (
	"flag"
	"os"
	"sync"

	"github.com/chrislusf/glow/driver/rsync"
	"github.com/chrislusf/glow/driver/scheduler"
	"github.com/chrislusf/glow/flame"
)

type DriverOption struct {
	ShouldStart bool
	Leader      string
}

func init() {
	var driverOption DriverOption
	flag.BoolVar(&driverOption.ShouldStart, "driver", false, "start in driver mode")
	flag.StringVar(&driverOption.Leader, "driver.leader", "localhost:8930", "leader server")

	flame.RegisterContextRunner(NewFlowContextDriver(&driverOption))
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
func (fcd *FlowContextDriver) Run(fc *flame.FlowContext) {

	// rsyncServer :=
	rsync.NewRsyncServer(os.Args[0])
	// rsyncServer.Start()

	taskGroups := scheduler.GroupTasks(fc)

	sched := scheduler.NewScheduler(fcd.option.Leader)
	go sched.Loop()

	// schedule to run the steps
	var wg sync.WaitGroup
	for _, taskGroup := range taskGroups {
		wg.Add(1)
		sched.EventChan <- scheduler.SubmittedTaskGroup{
			FlowContext: fc,
			TaskGroup:   taskGroup,
			WaitGroup:   &wg,
		}
	}
	wg.Wait()
}
