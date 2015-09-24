package resource

type ComputeResource struct {
	CPUCount     int
	CPULevel     int // higher number means higher compute power
	MemorySizeMB int
}

type ResourceOffer struct {
	ComputeResource
	ServerLocation Location
}

type ComputeRequest struct {
	ComputeResource
	DataLocation Location
}

type Location struct {
	DataCenter string
	Rack       string
	Server     string
	Port       int
}

func (c *ComputeRequest) Fit(r *ResourceOffer) (score float32, ok bool) {
	if c.CPUCount > r.CPUCount ||
		c.CPULevel > r.CPULevel ||
		c.MemorySizeMB > r.MemorySizeMB {
		return 0, false
	}
	return 10000 / c.DataLocation.Distance(r.ServerLocation), true
}

// the distance is a relative value, similar to network lantency
func (a Location) Distance(b Location) float32 {
	if a.DataCenter != b.DataCenter {
		return 1000
	}
	if a.Rack != b.Rack {
		return 100
	}
	if a.Server != b.Server {
		return 10
	}
	return 1
}
