package resource

import (
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
	DataCenters map[string]*DataCenter
	Resource    ComputeResource
	Allocated   ComputeResource
}
