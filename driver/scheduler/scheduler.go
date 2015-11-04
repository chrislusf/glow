// Schedule tasks to run on available resources assigned by master.
package scheduler

import (
	"github.com/chrislusf/glow/driver/scheduler/market"
)

type Scheduler struct {
	Leader       string
	EventChan    chan interface{}
	Market       *market.Market
	option       *SchedulerOption
	shardLocator *DatasetShardLocator
}

type SchedulerOption struct {
	DataCenter     string
	Rack           string
	TaskMemoryMB   int
	DriverPort     int
	Module         string
	ExecutableFile string
}

func NewScheduler(leader string, option *SchedulerOption) *Scheduler {
	s := &Scheduler{
		Leader:       leader,
		EventChan:    make(chan interface{}),
		Market:       market.NewMarket(),
		shardLocator: NewDatasetShardLocator(),
		option:       option,
	}
	s.Market.SetScoreFunction(s.Score).SetFetchFunction(s.Fetch)
	return s
}
