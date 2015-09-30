package leader

import (
	"fmt"
	"log"
	"sort"

	"github.com/chrislusf/glow/resource"
)

func (tl *TeamLeader) allocate(req *resource.AllocationRequest) (result *resource.AllocationResult) {
	result = &resource.AllocationResult{}
	dc, err := tl.findDataCenter(req)
	if err != nil {
		result.Error = err.Error()
		return
	}

	allocations := tl.findServers(dc, req)

	result.Allocations = allocations
	return
}

func (tl *TeamLeader) allocateServersOnRack(dc *resource.DataCenter, rack *resource.Rack, requests []*resource.ComputeRequest) (
	allocated []resource.Allocation, remainingRequests []*resource.ComputeRequest) {
	for _, agent := range rack.Agents {
		available := agent.Resource.Minus(agent.Allocated)
		hasAllocation := true
		for available.GreaterThanZero() && hasAllocation {
			hasAllocation = false
			for _, request := range requests {
				if available.Covers(request.ComputeResource) {
					allocated = append(allocated, resource.Allocation{
						Location:  agent.Location,
						Allocated: request.ComputeResource,
					})
					agent.Allocated = agent.Allocated.Plus(request.ComputeResource)
					rack.Allocated = rack.Allocated.Plus(request.ComputeResource)
					dc.Allocated = dc.Allocated.Plus(request.ComputeResource)
					tl.LeaderResource.Topology.Allocated = tl.LeaderResource.Topology.Allocated.Plus(request.ComputeResource)
					available = available.Minus(request.ComputeResource)
					hasAllocation = true
				} else {
					remainingRequests = append(remainingRequests, request)
				}
			}
		}
	}
	return
}

func (tl *TeamLeader) findServers(dc *resource.DataCenter, req *resource.AllocationRequest) (ret []resource.Allocation) {
	// sort racks by unallocated resources
	racks := make([]*resource.Rack, 0, len(dc.Racks))
	for _, rack := range dc.Racks {
		racks = append(racks, rack)
	}
	sort.Sort(ByAvailableResources(racks))

	requests := make([]*resource.ComputeRequest, 0, len(req.Requests))
	for _, request := range req.Requests {
		requests = append(requests, &request)
	}
	sort.Sort(ByRequestedResources(requests))

	for _, rack := range racks {
		allocated, requests := tl.allocateServersOnRack(dc, rack, requests)
		ret = append(ret, allocated...)
		if len(requests) == 0 {
			break
		}
	}
	return
}

func (tl *TeamLeader) findDataCenter(req *resource.AllocationRequest) (*resource.DataCenter, error) {
	// calculate total resource requested
	var totalComputeResource resource.ComputeResource
	for _, cr := range req.Requests {
		totalComputeResource = totalComputeResource.Plus(cr.ComputeResource)
	}

	// check preferred data center
	dcName := ""
	for _, cr := range req.Requests {
		for _, input := range cr.Inputs {
			dcName = input.Location.DataCenter
		}
	}
	if dcName != "" {
		dc, hasDc := tl.LeaderResource.Topology.DataCenters[dcName]
		if !hasDc {
			log.Fatalf("Failed to find existing data center: %s", dcName)
		}
		if dc.Resource.Minus(dc.Allocated).Covers(totalComputeResource) {
			return dc, nil
		}
		return nil, fmt.Errorf("Data center %s is busy for:%v", dcName, totalComputeResource)
	}

	// ensure one data center has enough resources
	found := false
	for _, dc := range tl.LeaderResource.Topology.DataCenters {
		if dc.Resource.Covers(totalComputeResource) {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("Total compute resource is too big for any data center:%+v", totalComputeResource)
	}

	// find a data center with unallocated resources
	for _, dc := range tl.LeaderResource.Topology.DataCenters {
		// fmt.Printf("dc has: %+v, used:%+v, totalComputeResource: %+v\n", dc.Resource, dc.Allocated, totalComputeResource)
		if dc.Resource.Minus(dc.Allocated).Covers(totalComputeResource) {
			return dc, nil
		}
	}
	return nil, fmt.Errorf("All data centers are busy for:%v", totalComputeResource)
}

type ByAvailableResources []*resource.Rack

func (s ByAvailableResources) Len() int      { return len(s) }
func (s ByAvailableResources) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ByAvailableResources) Less(i, j int) bool {
	return s[i].Resource.Minus(s[i].Allocated).Covers(s[j].Resource.Minus(s[j].Allocated))
}

type ByRequestedResources []*resource.ComputeRequest

func (s ByRequestedResources) Len() int      { return len(s) }
func (s ByRequestedResources) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ByRequestedResources) Less(i, j int) bool {
	return s[i].ComputeResource.Covers(s[j].ComputeResource)
}
