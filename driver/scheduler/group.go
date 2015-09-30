package scheduler

import (
	"log"

	"github.com/chrislusf/glow/flow"
)

type TaskGroupStatus int

const (
	New TaskGroupStatus = iota
	Waiting
	Scheduled
	Completed
	Failed
)

type TaskGroup struct {
	Id              int
	Tasks           []*flow.Task
	Parents         []*TaskGroup
	ParentStepGroup *StepGroup
	Status          TaskGroupStatus
}

type StepGroup struct {
	Steps      []*flow.Step
	Parents    []*StepGroup
	TaskGroups []*TaskGroup
}

func GroupTasks(fc *flow.FlowContext) []*TaskGroup {
	stepGroups := translateToStepGroups(fc)
	return translateToTaskGroups(stepGroups)
}

func findAncestorStepId(step *flow.Step) (int, bool) {
	current := step
	taskCount := len(step.Tasks)
	var next *flow.Step
	for current.Type == flow.Local && taskCount == len(current.Tasks) {
		if len(current.Inputs) > 1 {
			log.Panic("local step should not have more than 1 input")
		}
		if len(current.Inputs) == 0 {
			break
		}
		next = current.Inputs[0].Step
		if next.Type != flow.Local || taskCount != len(next.Tasks) {
			break
		}
		current = next
	}
	return current.Id, true
}

// group local steps into one step group
func translateToStepGroups(fc *flow.FlowContext) []*StepGroup {
	// use array instead of map to ensure consistent ordering
	stepId2StepGroup := make([]*StepGroup, len(fc.Steps))
	for _, step := range fc.Steps {
		// println("step:", step.Id, "starting...")
		stepId, foundStepId := findAncestorStepId(step)
		if !foundStepId {
			// println("step:", step.Id, "Not found stepId.")
			continue
		}
		// println("step:", step.Id, "dataset id", stepId)
		if stepId2StepGroup[stepId] == nil {
			stepId2StepGroup[stepId] = NewStepGroup()
			for _, ds := range step.Inputs {
				parentDsId, hasParentIdId := findAncestorStepId(ds.Step)
				if !hasParentIdId {
					// since we add steps following the same order as the code
					log.Panic("parent StepGroup should already be in the map")
				}
				parentSg := stepId2StepGroup[parentDsId]
				if parentSg == nil {
					// since we add steps following the same order as the code
					log.Panic("parent StepGroup should already be in the map")
				}
				stepId2StepGroup[stepId].AddParent(parentSg)
			}
		}
		stepId2StepGroup[stepId].AddStep(step)
	}
	// shrink
	var ret []*StepGroup
	for _, stepGroup := range stepId2StepGroup {
		if stepGroup == nil || len(stepGroup.Steps) == 0 {
			continue
		}
		ret = append(ret, stepGroup)
	}
	return ret
}

// group local tasks into one task group
func translateToTaskGroups(stepId2StepGroup []*StepGroup) (ret []*TaskGroup) {
	for _, stepGroup := range stepId2StepGroup {
		assertSameNumberOfTasks(stepGroup.Steps)
		count := len(stepGroup.Steps[0].Tasks)
		for i := 0; i < count; i++ {
			tg := NewTaskGroup()
			for _, step := range stepGroup.Steps {
				tg.AddTask(step.Tasks[i])
			}
			// depends on the previous step group
			// MAYBE IMPROVEMENT: depends on a subset of previus shards
			tg.ParentStepGroup = stepGroup
			stepGroup.TaskGroups = append(stepGroup.TaskGroups, tg)
			tg.Id = len(ret)
			ret = append(ret, tg)
		}
	}
	return
}

func assertSameNumberOfTasks(steps []*flow.Step) {
	if len(steps) == 0 {
		return
	}
	count := len(steps[0].Tasks)
	for _, step := range steps {
		if count != len(step.Tasks) {
			log.Fatalf("This should not happen: step %d has %d tasks, but step %d has %d tasks.", steps[0].Id, count, step.Id, len(step.Tasks))
		}
	}
}

func NewStepGroup() *StepGroup {
	return &StepGroup{}
}

func (t *StepGroup) AddStep(Step *flow.Step) *StepGroup {
	t.Steps = append(t.Steps, Step)
	return t
}

func (t *StepGroup) AddParent(parent *StepGroup) *StepGroup {
	t.Parents = append(t.Parents, parent)
	return t
}

func NewTaskGroup() *TaskGroup {
	return &TaskGroup{}
}

func (t *TaskGroup) AddTask(task *flow.Task) *TaskGroup {
	t.Tasks = append(t.Tasks, task)
	return t
}

func (t *TaskGroup) AddParent(parent *TaskGroup) *TaskGroup {
	t.Parents = append(t.Parents, parent)
	return t
}
