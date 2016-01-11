// Schedule tasks to run on available resources assigned by master.
package scheduler

import (
	"sync"
	"time"

	"github.com/chrislusf/glow/driver/scheduler/market"
)

type Scheduler struct {
	sync.Mutex

	Leader                 string
	EventChan              chan interface{}
	Market                 *market.Market
	option                 *SchedulerOption
	shardLocator           *DatasetShardLocator
	RemoteExecutorStatuses map[int32]*RemoteExecutorStatus
}

type RemoteExecutorStatus struct {
	RequestTime  time.Time
	InputLength  int
	OutputLength int
	ReadyTime    time.Time
	RunTime      time.Time
	StopTime     time.Time
}

type SchedulerOption struct {
	DataCenter         string
	Rack               string
	TaskMemoryMB       int
	DriverPort         int
	Module             string
	ExecutableFile     string
	ExecutableFileHash string
}

func NewScheduler(leader string, option *SchedulerOption) *Scheduler {
	s := &Scheduler{
		Leader:                 leader,
		EventChan:              make(chan interface{}),
		Market:                 market.NewMarket(),
		shardLocator:           NewDatasetShardLocator(option.ExecutableFileHash),
		option:                 option,
		RemoteExecutorStatuses: make(map[int32]*RemoteExecutorStatus),
	}
	s.Market.SetScoreFunction(s.Score).SetFetchFunction(s.Fetch)
	return s
}

func (s *Scheduler) getRemoteExecutorStatus(id int32) *RemoteExecutorStatus {
	s.Lock()
	defer s.Unlock()

	status, ok := s.RemoteExecutorStatuses[id]
	if ok {
		return status
	}
	status = &RemoteExecutorStatus{}
	s.RemoteExecutorStatuses[id] = status
	return status
}
