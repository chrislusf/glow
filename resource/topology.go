package resource

import (
	"sync"
	"time"
)

type AgentInformation struct {
	Location      Location
	LastHeartBeat time.Time
	Resource      ComputeResource
	Allocated     ComputeResource
}

type Rack struct {
	Name      string
	Agents    map[string]*AgentInformation
	Resource  ComputeResource
	Allocated ComputeResource
}

type DataCenter struct {
	Name      string
	Racks     map[string]*Rack
	Resource  ComputeResource
	Allocated ComputeResource
}

type Topology struct {
	Resource    ComputeResource
	Allocated   ComputeResource
	lock        sync.RWMutex
	dataCenters map[string]*DataCenter
}

func NewTopology() *Topology {
	return &Topology{
		dataCenters: make(map[string]*DataCenter),
	}
}

func (tp *Topology) ContainsDataCenters() bool {
	tp.lock.RLock()
	defer tp.lock.RUnlock()
	return len(tp.dataCenters) == 0
}

func (tp *Topology) GetDataCenter(name string) (*DataCenter, bool) {
	tp.lock.RLock()
	defer tp.lock.RUnlock()

	dc, ok := tp.dataCenters[name]
	return dc, ok
}

func (tp *Topology) AddDataCenter(dc *DataCenter) {
	tp.lock.Lock()
	defer tp.lock.Unlock()

	tp.dataCenters[dc.Name] = dc
}

func (tp *Topology) DataCenters() map[string]*DataCenter {
	tp.lock.RLock()
	defer tp.lock.RUnlock()

	s := make(map[string]*DataCenter, len(tp.dataCenters))
	for k, v := range tp.dataCenters {
		s[k] = v
	}
	return s
}
