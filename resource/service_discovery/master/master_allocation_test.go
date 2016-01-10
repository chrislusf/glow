package master

import (
	"fmt"
	"testing"

	"github.com/chrislusf/glow/resource"
)

func TestAllocation1(t *testing.T) {

	lr := NewMasterResource()
	lr.UpdateAgentInformation(&resource.AgentInformation{
		Location: resource.Location{
			DataCenter: "dc1",
			Rack:       "rack1",
			Server:     "server1",
			Port:       1111,
		},
		Resource: resource.ComputeResource{
			CPUCount: 1,
			CPULevel: 1,
			MemoryMB: 1024,
		},
	})
	lr.UpdateAgentInformation(&resource.AgentInformation{
		Location: resource.Location{
			DataCenter: "dc1",
			Rack:       "rack2",
			Server:     "server2",
			Port:       1111,
		},
		Resource: resource.ComputeResource{
			CPUCount: 1,
			CPULevel: 1,
			MemoryMB: 1024,
		},
	})
	lr.UpdateAgentInformation(&resource.AgentInformation{
		Location: resource.Location{
			DataCenter: "dc2",
			Rack:       "rack3",
			Server:     "server3",
			Port:       1111,
		},
		Resource: resource.ComputeResource{
			CPUCount: 16,
			CPULevel: 1,
			MemoryMB: 1024,
		},
	})

	req := &resource.AllocationRequest{
		Requests: []resource.ComputeRequest{
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 1024,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 1024,
				},
				Inputs: nil,
			},
		},
	}

	tl := &TeamMaster{}
	tl.channels = &NamedChannelMap{name2Chans: make(map[string][]*ChannelInformation)}
	tl.MasterResource = lr

	result := tl.allocate(req)
	t.Logf("Result: %+v", result)

	fmt.Printf("===========Start test 2============\n")
	req2 := &resource.AllocationRequest{
		Requests: []resource.ComputeRequest{
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
		},
	}
	result2 := tl.allocate(req2)
	t.Logf("Result: %+v", result2)

}

func TestAllocation2(t *testing.T) {

	lr := NewMasterResource()
	lr.UpdateAgentInformation(&resource.AgentInformation{
		Location: resource.Location{
			DataCenter: "dc1",
			Rack:       "rack1",
			Server:     "server1",
			Port:       1111,
		},
		Resource: resource.ComputeResource{
			CPUCount: 16,
			CPULevel: 1,
			MemoryMB: 1024,
		},
	})

	tl := &TeamMaster{}
	tl.channels = &NamedChannelMap{name2Chans: make(map[string][]*ChannelInformation)}
	tl.MasterResource = lr

	req := &resource.AllocationRequest{
		Requests: []resource.ComputeRequest{
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
		},
	}
	result := tl.allocate(req)
	t.Logf("Result2: %+v", result)

}
