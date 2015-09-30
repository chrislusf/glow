package scheduler

import (
	"sync"

	"github.com/chrislusf/glow/driver/scheduler/market"
	"github.com/chrislusf/glow/resource"
)

type Scheduler struct {
	Leader                string
	EventChan             chan interface{}
	resourceFetcher       *ResourceFetcher
	lock                  sync.Mutex
	datasetShard2Location map[string]resource.Location
	Market                *market.Market
}

type SchedulerOption struct {
	DataCenter string
	Rack       string
}

func NewScheduler(leader string, option *SchedulerOption) *Scheduler {
	s := &Scheduler{
		Leader:                leader,
		EventChan:             make(chan interface{}),
		resourceFetcher:       NewResourceFetcher(),
		datasetShard2Location: make(map[string]resource.Location),
		Market:                market.NewMarket(),
	}
	s.InitMarket()
	return s
}
