package driver

import (
	"github.com/chrislusf/glow/driver/plan"
	"github.com/chrislusf/glow/driver/scheduler"
	"github.com/chrislusf/glow/flow"
)

func (fcd *FlowContextDriver) OnInterrupt(
	fc *flow.FlowContext,
	taskGroups []*plan.TaskGroup,
	sched *scheduler.Scheduler) {
}
