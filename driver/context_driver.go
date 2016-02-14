// Pacakge driver coordinates distributed execution.
package driver

import (
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/chrislusf/glow/driver/plan"
	"github.com/chrislusf/glow/driver/rsync"
	"github.com/chrislusf/glow/driver/scheduler"
	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/netchan"
	"github.com/chrislusf/glow/util"
)

type DriverOption struct {
	ShouldStart   bool
	Leader        string
	DataCenter    string
	Rack          string
	PlotOutput    bool
	TaskMemoryMB  int
	FlowBid       float64
	Module        string
	RelatedFiles  string
	ShowFlowStats bool
	Host          string
	Port          int
	CertFiles     netchan.CertFiles
}

var driverOption DriverOption

func init() {
	flag.BoolVar(&driverOption.ShouldStart, "glow", false, "start in driver mode")
	flag.StringVar(&driverOption.Leader, "glow.leader", "localhost:8930", "leader server")
	flag.StringVar(&driverOption.DataCenter, "glow.dataCenter", "", "preferred data center name")
	flag.StringVar(&driverOption.Rack, "glow.rack", "", "preferred rack name")
	flag.IntVar(&driverOption.TaskMemoryMB, "glow.task.memoryMB", 64, "request one task memory size in MB")
	flag.Float64Var(&driverOption.FlowBid, "glow.flow.bid", 100.0, "total bid price in a flow to compete for resources")
	flag.BoolVar(&driverOption.PlotOutput, "glow.flow.plot", false, "print out task group flow in graphviz dot format")
	flag.StringVar(&driverOption.Module, "glow.module", "", "a name to group related jobs together on agent")
	flag.StringVar(&driverOption.RelatedFiles, "glow.related.files", "", strconv.QuoteRune(os.PathListSeparator)+" separated list of files")
	flag.BoolVar(&driverOption.ShowFlowStats, "glow.flow.stat", false, "show flow details at the end of execution")
	flag.StringVar(&driverOption.Host, "glow.driver.host", "", "driver runs on this host address. Required in 2-way SSL mode.")
	flag.IntVar(&driverOption.Port, "glow.driver.port", 0, "driver listens on this port to copy files to agents. Required to specify and open this port.")
	flag.StringVar(&driverOption.CertFiles.CertFile, "cert.file", "", "A PEM eoncoded certificate file")
	flag.StringVar(&driverOption.CertFiles.KeyFile, "key.file", "", "A PEM encoded private key file")
	flag.StringVar(&driverOption.CertFiles.CaFile, "ca.file", "", "A PEM eoncoded CA's certificate file")

	flow.RegisterContextRunner(NewFlowContextDriver(&driverOption))
}

type FlowContextDriver struct {
	Option *DriverOption

	stepGroups []*plan.StepGroup
	taskGroups []*plan.TaskGroup
}

func NewFlowContextDriver(option *DriverOption) *FlowContextDriver {
	return &FlowContextDriver{Option: option}
}

func (fcd *FlowContextDriver) IsDriverMode() bool {
	return fcd.Option.ShouldStart
}

func (fcd *FlowContextDriver) IsDriverPlotMode() bool {
	return fcd.Option.PlotOutput
}

func (fcd *FlowContextDriver) checkParameters() {
	if fcd.Option.CertFiles.IsEnabled() {
		if fcd.Option.Host == "" {
			log.Fatalf("Usage Note: -glow.driver.host option is needed and must match CN in the certificate.")
		}
	}
}

func (fcd *FlowContextDriver) Plot(fc *flow.FlowContext) {
	_, fcd.taskGroups = plan.GroupTasks(fc)
	plan.PlotGraph(fcd.taskGroups, fc)
}

// driver runs on local, controlling all tasks
func (fcd *FlowContextDriver) Run(fc *flow.FlowContext) {

	fcd.checkParameters()

	// task fusion to minimize disk IO
	fcd.stepGroups, fcd.taskGroups = plan.GroupTasks(fc)
	// plot the execution graph
	if fcd.Option.PlotOutput {
		plan.PlotGraph(fcd.taskGroups, fc)
		return
	}

	tlsConfig := fcd.Option.CertFiles.MakeTLSConfig()
	util.SetupHttpClient(tlsConfig)

	// start server to serve files to agents to run exectuors
	rsyncServer, err := rsync.NewRsyncServer(os.Args[0], fcd.Option.RelatedFileNames())
	if err != nil {
		log.Fatalf("Failed to start local server: %v", err)
	}
	rsyncServer.StartRsyncServer(tlsConfig, fcd.Option.Host+":"+strconv.Itoa(fcd.Option.Port))

	driverHost := fcd.Option.Host

	// create thes cheduler
	sched := scheduler.NewScheduler(
		fcd.Option.Leader,
		&scheduler.SchedulerOption{
			DataCenter:         fcd.Option.DataCenter,
			Rack:               fcd.Option.Rack,
			TaskMemoryMB:       fcd.Option.TaskMemoryMB,
			DriverHost:         driverHost,
			DriverPort:         rsyncServer.Port,
			Module:             fcd.Option.Module,
			ExecutableFile:     os.Args[0],
			ExecutableFileHash: rsyncServer.ExecutableFileHash(),
			TlsConfig:          tlsConfig,
		},
	)

	// best effort to clean data on agent disk
	// this may need more improvements
	defer fcd.Cleanup(sched, fc)

	go sched.EventLoop()

	flow.OnInterrupt(func() {
		fcd.OnInterrupt(fc, sched)
	}, func() {
		fcd.OnExit(fc, sched)
	})

	// schedule to run the steps
	var wg sync.WaitGroup
	for _, taskGroup := range fcd.taskGroups {
		wg.Add(1)
		sched.EventChan <- scheduler.SubmitTaskGroup{
			FlowContext: fc,
			TaskGroup:   taskGroup,
			Bid:         fcd.Option.FlowBid / float64(len(fcd.taskGroups)),
			WaitGroup:   &wg,
		}
	}
	go sched.Market.FetcherLoop()

	wg.Wait()

	fcd.CloseOutputChannels(fc)

	if fcd.Option.ShowFlowStats {
		fcd.ShowFlowStatus(fc, sched)
	}

}

func (fcd *FlowContextDriver) Cleanup(sched *scheduler.Scheduler, fc *flow.FlowContext) {
	var wg sync.WaitGroup
	wg.Add(1)
	sched.EventChan <- scheduler.ReleaseTaskGroupInputs{
		FlowContext: fc,
		TaskGroups:  fcd.taskGroups,
		WaitGroup:   &wg,
	}

	wg.Wait()
}

func (fcd *FlowContextDriver) CloseOutputChannels(fc *flow.FlowContext) {
	for _, ds := range fc.Datasets {
		for _, ch := range ds.ExternalOutputChans {
			ch.Close()
		}
	}
}

func (option *DriverOption) RelatedFileNames() []string {
	if option.RelatedFiles != "" {
		return strings.Split(option.RelatedFiles, strconv.QuoteRune(os.PathListSeparator))
	}
	return []string{}
}
