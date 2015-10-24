package plan

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/chrislusf/glow/flow"
)

type FlowGraph struct {
	flowContext *flow.FlowContext
	taskGroups  []*TaskGroup
	out         *bytes.Buffer
}

func PlotGraph(taskGroups []*TaskGroup, fc *flow.FlowContext) {
	var buffer bytes.Buffer
	fg := &FlowGraph{fc, taskGroups, &buffer}
	fg.plot()

	fmt.Println(buffer.String())
}

/*
digraph G {
	subgraph cluster0 {
		node [style=filled,color=white];
		style=filled;
		color=lightgrey;
		a0 -> a1 -> a2 -> a3;
		label = "process #1";
	}
	subgraph cluster1 {
		node [style=filled];
		b0 -> b1 -> b2 -> b3;
		label = "process #2";
		color=blue
	}
	start -> a0;
	start -> b0;
	a1 -> b3;
	b2 -> a3;
	a3 -> a0;
	a3 -> end;
	b3 -> end;

	start [shape=Mdiamond];
	end [shape=Msquare];
}
*/
func (fg *FlowGraph) plot() {
	fg.println("digraph glow {")
	prefix := "  "
	for _, tg := range fg.taskGroups {
		fg.printTaskGroup(tg, prefix)
	}
	hasStart, hasEnd := false, false
	for _, tg := range fg.taskGroups {
		firstTask, lastTask := tg.Tasks[0], tg.Tasks[len(tg.Tasks)-1]
		if firstTask.Inputs == nil {
			ds := firstTask.Outputs[0].Parent
			if len(ds.ExternalInputChans) == 0 {
				fg.w(prefix).w("start -> ").t(firstTask).println(";")
				hasStart = true
			} else {
				for i, _ := range ds.ExternalInputChans {
					fg.w(prefix).input(firstTask, i, len(ds.ExternalInputChans)).println(" [shape=doublecircle];")
					fg.w(prefix).input(firstTask, i, len(ds.ExternalInputChans)).w(" -> ").t(firstTask).println(";")
				}
			}
		} else {
			for _, dss := range firstTask.Inputs {
				fg.w(prefix).d(dss).w(" -> ").t(firstTask).println(";")
			}
		}

		if lastTask.Outputs == nil {
			hasEnd = true
			fg.w(prefix).t(lastTask).println(" -> end;")
		} else {
			for _, dss := range lastTask.Outputs {
				fg.w(prefix).t(lastTask).w(" -> ").d(dss).println(";")
			}
		}
	}

	for _, ds := range fg.flowContext.Datasets {
		if len(ds.ExternalOutputChans) > 0 {
			fg.w(prefix).output(ds).println(" [shape=doublecircle];")
			for _, dss := range ds.Shards {
				fg.w(prefix).d(dss).w(" -> ").output(ds).println(";")
			}
		}
	}

	fg.w(prefix).println("center=true;")
	fg.w(prefix).println("compound=true;")
	if hasStart {
		fg.w(prefix).println("start [shape=Mdiamond];")
	}
	if hasEnd {
		fg.w(prefix).println("end [shape=Msquare];")
	}

	fg.println("}")
}

func (fg *FlowGraph) println(t string) *FlowGraph {
	fg.out.WriteString(t)
	fg.out.WriteString("\n")
	return fg
}
func (fg *FlowGraph) print(t string) *FlowGraph {
	fg.out.WriteString(t)
	return fg
}
func (fg *FlowGraph) w(t string) *FlowGraph {
	fg.out.WriteString(t)
	return fg
}
func (fg *FlowGraph) i(x int) *FlowGraph {
	fg.out.WriteString(strconv.Itoa(x))
	return fg
}
func (fg *FlowGraph) t(t *flow.Task) *FlowGraph {
	if t.Step.Name != "" {
		fg.w(t.Step.Name)
	} else {
		fg.w("t")
	}
	fg.i(t.Step.Id)
	if len(t.Step.Tasks) > 1 {
		fg.w("_").i(t.Id).w("_").i(len(t.Step.Tasks))
	}
	return fg
}
func (fg *FlowGraph) d(dss *flow.DatasetShard) *FlowGraph {
	fg.w("d").i(dss.Parent.Id).w("_").i(dss.Id)
	return fg
}
func (fg *FlowGraph) input(t *flow.Task, i, length int) *FlowGraph {
	fg.w("input").i(t.Id)
	return fg
}
func (fg *FlowGraph) output(ds *flow.Dataset) *FlowGraph {
	fg.w("output").i(ds.Id)
	return fg
}

/*
	subgraph cluster0 {
		node [style=filled,color=white];
		style=filled;
		color=lightgrey;
		a0 -> a1 -> a2 -> a3;
		label = "process #1";
	}
*/

func (fg *FlowGraph) printTaskGroup(tg *TaskGroup, p string) {
	fg.w(p).w("subgraph group_").i(tg.Id).println("{")
	fg.w(p).w(p).println("node [style=filled,color=white];")
	fg.w(p).w(p).println("style=filled;")
	fg.w(p).w(p).println("color=lightgrey;")
	for i, task := range tg.Tasks {
		if i == 0 {
			fg.w(p).w(p).t(task)
			continue
		}
		fg.w(" -> ")
		fg.t(task)
	}
	fg.println(";")
	fg.w(p).w(p).w("label = \"group_").i(tg.Id).println("\";")
	fg.w(p).println("}")
}
