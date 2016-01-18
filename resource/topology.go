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
	sync.RWMutex
	Name      string
	Agents    map[string]*AgentInformation
	Resource  ComputeResource
	Allocated ComputeResource
}

type DataCenter struct {
	sync.RWMutex
	Name      string
	racks     map[string]*Rack
	Resource  ComputeResource
	Allocated ComputeResource
}

type Topology struct {
	Resource    ComputeResource
	Allocated   ComputeResource
	sync.RWMutex
	dataCenters map[string]*DataCenter
}

func NewTopology() *Topology {
	return &Topology{
		dataCenters: make(map[string]*DataCenter),
	}
}

func NewDataCenter(name string) *DataCenter {
	return &DataCenter{
		Name: name,
		racks: make(map[string]*Rack),
	}
}

func (tp *Topology) ContainsDataCenters() bool {
	tp.RLock()
	defer tp.RUnlock()
	return len(tp.dataCenters) == 0
}

func (tp *Topology) GetDataCenter(name string) (*DataCenter, bool) {
	tp.RLock()
	defer tp.RUnlock()

	dc, ok := tp.dataCenters[name]
	return dc, ok
}

func (dc *DataCenter) GetRack(name string) (*Rack, bool) {
	dc.RLock()
	defer dc.RUnlock()

	rack, ok := dc.racks[name]
	return rack, ok
}

func (tp *Topology) AddDataCenter(dc *DataCenter) {
	tp.Lock()
	defer tp.Unlock()

	tp.dataCenters[dc.Name] = dc
}

func (tp *Topology) DataCenters() map[string]*DataCenter {
	tp.RLock()
	defer tp.RUnlock()

	s := make(map[string]*DataCenter, len(tp.dataCenters))
	for k, v := range tp.dataCenters {
		s[k] = v
	}
	return s
}

func (dc *DataCenter) Racks() map[string]*Rack {
	dc.RLock()
	defer dc.RUnlock()

	s := make(map[string]*Rack, len(dc.racks))
	for k, v := range dc.racks {
		s[k] = v
	}
	return s
}

func (dc *DataCenter) AddRack(rack *Rack) {
	dc.Lock()
	defer dc.Unlock()

	dc.racks[rack.Name] = rack
}
