package scheduler

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"time"

	"github.com/chrislusf/glow/driver/scheduler/market"
	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/resource"
	"github.com/chrislusf/glow/util"
)

type ResourceFetcher struct {
	a, b         int
	isRequesting bool
}

func NewResourceFetcher() *ResourceFetcher {
	return &ResourceFetcher{
		a: 0,
		b: 1,
	}
}

func (r *ResourceFetcher) requestSize() (int, bool) {
	if r.isRequesting {
		return 0, false
	}
	r.isRequesting = true
	ret := r.b
	r.a, r.b = r.b, r.a+r.b
	return ret, true
}

type DatasetShardAllocated struct {
	DatasetShard *flow.DatasetShard
	Location     resource.Location
}

// Requirement is TaskGroup
// Object is Agent's Location
func (s *Scheduler) InitMarket() {
	s.Market.SetScoreFunction(func(r market.Requirement, bid int, obj market.Object) float64 {
		tg, loc := r.(*TaskGroup), obj.(resource.Allocation).Location
		firstTask := tg.Tasks[0]
		cost := float64(1)
		for _, input := range firstTask.Inputs {
			dataLocation, found := s.datasetShard2Location[input.Name()]
			if !found {
				// log.Printf("Strange1: %s not allocated yet.", input.Name())
				continue
			}
			cost += dataLocation.Distance(loc)
		}
		return float64(bid) / cost
	}).SetFetchFunction(func(demands []market.Demand) {
		var request resource.AllocationRequest
		for _, d := range demands {
			demand := d.Requirement.(*TaskGroup)
			request.Requests = append(request.Requests, resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 64,
				},
				Inputs: s.findTaskGroupInputs(demand),
			})
		}

		result, err := Assign(s.Leader, &request)
		if err != nil {
			log.Printf("%s Failed to allocate: %v", s.Leader, err)
		} else {
			if len(result.Allocations) == 0 {
				log.Printf("%s Failed to allocate any server.", s.Leader)
				time.Sleep(time.Millisecond * time.Duration(2000+rand.Int63n(1000)))
			} else {
				log.Printf("%s allocated %d servers.", s.Leader, len(result.Allocations))
				for _, allocation := range result.Allocations {
					s.Market.AddSupply(market.Supply{
						Object: allocation,
					})
				}
			}
		}

	})
}

func (s *Scheduler) findTaskGroupInputs(tg *TaskGroup) (ret []resource.DataResource) {
	firstTask := tg.Tasks[0]
	for _, input := range firstTask.Inputs {
		dataLocation, found := s.datasetShard2Location[input.Name()]
		if !found {
			// log.Printf("Strange2: %s not allocated yet.", input.Name())
			continue
		}
		ret = append(ret, resource.DataResource{
			Location:   dataLocation,
			DataSizeMB: 1, // TODO: read previous run's size
		})
	}
	return
}

func Assign(leader string, request *resource.AllocationRequest) (*resource.AllocationResult, error) {
	values := make(url.Values)
	requestBlob, _ := json.Marshal(request)
	values.Add("request", string(requestBlob))
	jsonBlob, err := util.Post("http://"+leader+"/agent/assign", values)
	if err != nil {
		return nil, err
	}
	var ret resource.AllocationResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, fmt.Errorf("/agent/assign result JSON unmarshal error:%v, json:%s", err, string(jsonBlob))
	}
	if ret.Error != "" {
		return nil, fmt.Errorf("/agent/assign error:%v, json:%s", ret.Error, string(jsonBlob))
	}
	return &ret, nil
}
