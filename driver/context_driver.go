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
	ShouldStart  bool
	Leader       string
	DataCenter   string
	Rack         string
	PlotOutput   bool
	TaskMemoryMB int
	FlowBid      float64
}

func init() {
	var driverOption DriverOption
	flag.BoolVar(&driverOption.ShouldStart, "driver", false, "start in driver mode")
	flag.StringVar(&driverOption.Leader, "driver.leader", "localhost:8930", "leader server")
	flag.StringVar(&driverOption.DataCenter, "driver.dataCenter", "", "preferred data center name")
	flag.StringVar(&driverOption.Rack, "driver.rack", "", "preferred rack name")
	flag.IntVar(&driverOption.TaskMemoryMB, "driver.task.memoryMB", 64, "request one task memory size in MB")
	flag.Float64Var(&driverOption.FlowBid, "driver.flow.bid", 100.0, "total bid price in a flow to compete for resources")
	flag.BoolVar(&driverOption.PlotOutput, "driver.plot.flow", false, "print out task group flow in graphviz dot format")

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

func (fcd *FlowContextDriver) IsDriverPlotMode() bool {
	return fcd.option.PlotOutput
}

func (fcd *FlowContextDriver) Plot(fc *flow.FlowContext) {
	taskGroups := scheduler.GroupTasks(fc)
	scheduler.PlotGraph(taskGroups, fc)
}

// driver runs on local, controlling all tasks
func (fcd *FlowContextDriver) Run(fc *flow.FlowContext) {

	taskGroups := scheduler.GroupTasks(fc)
	if fcd.option.PlotOutput {
		scheduler.PlotGraph(taskGroups, fc)
		return
	}

	// rsyncServer :=
	rsync.NewRsyncServer(os.Args[0])
	// rsyncServer.Start()

	sched := scheduler.NewScheduler(
		fcd.option.Leader,
		&scheduler.SchedulerOption{
			DataCenter:   fcd.option.DataCenter,
			Rack:         fcd.option.Rack,
			TaskMemoryMB: fcd.option.TaskMemoryMB,
		},
	)
	defer fcd.Cleanup(sched, fc, taskGroups)

	go sched.EventLoop()

	// schedule to run the steps
	var wg sync.WaitGroup
	for _, taskGroup := range taskGroups {
		wg.Add(1)
		sched.EventChan <- scheduler.SubmitTaskGroup{
			FlowContext: fc,
			TaskGroup:   taskGroup,
			Bid:         fcd.option.FlowBid / float64(len(taskGroups)),
			WaitGroup:   &wg,
		}
	}
	go sched.Market.FetcherLoop()

	wg.Wait()

	for _, ds := range fc.Datasets {
		if len(ds.OutputChans) > 0 {
			for _, ch := range ds.OutputChans {
				ch.Close()
			}
		}
	}
}

func (fcd *FlowContextDriver) Cleanup(sched *scheduler.Scheduler, fc *flow.FlowContext, taskGroups []*scheduler.TaskGroup) {
	var wg sync.WaitGroup
	wg.Add(1)
	sched.EventChan <- scheduler.ReleaseTaskGroupInputs{
		FlowContext: fc,
		TaskGroups:  taskGroups,
		WaitGroup:   &wg,
	}

	wg.Wait()
}
