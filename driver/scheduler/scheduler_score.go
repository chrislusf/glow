package scheduler

import (
	"github.com/chrislusf/glow/driver/plan"
	"github.com/chrislusf/glow/driver/scheduler/market"
	"github.com/chrislusf/glow/resource"
)

func (s *Scheduler) Score(r market.Requirement, bid float64, obj market.Object) float64 {
	tg, loc := r.(*plan.TaskGroup), obj.(resource.Allocation).Location
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
}
