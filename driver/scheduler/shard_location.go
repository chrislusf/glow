package scheduler

import (
	"bytes"
	"sync"

	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/resource"
)

type DatasetShardLocator struct {
	sync.Mutex
	datasetShard2Location map[string]resource.Location
	waitForAllInputs      *sync.Cond
}

func NewDatasetShardLocator() *DatasetShardLocator {
	l := &DatasetShardLocator{
		datasetShard2Location: make(map[string]resource.Location),
	}
	l.waitForAllInputs = sync.NewCond(l)
	return l
}

func (l *DatasetShardLocator) GetShardLocation(shardName string) (resource.Location, bool) {
	loc, hasValue := l.datasetShard2Location[shardName]
	return loc, hasValue
}

func (l *DatasetShardLocator) SetShardLocation(name string, location resource.Location) {
	l.Lock()
	defer l.Unlock()

	// fmt.Printf("shard %s is at %s\n", name, location.URL())
	l.datasetShard2Location[name] = location
	l.waitForAllInputs.Broadcast()
}

func (l *DatasetShardLocator) allInputsAreRegistered(task *flow.Task) bool {
	for _, input := range task.Inputs {
		if _, hasValue := l.GetShardLocation(input.Name()); !hasValue {
			// fmt.Printf("%s's input %s is not ready\n", task.Name(), input.Name())
			return false
		}
	}
	return true
}

func (l *DatasetShardLocator) waitForInputDatasetShardLocations(task *flow.Task) {
	l.Lock()
	defer l.Unlock()

	for !l.allInputsAreRegistered(task) {
		l.waitForAllInputs.Wait()
	}
}

func (l *DatasetShardLocator) allInputLocations(task *flow.Task) string {
	l.Lock()
	defer l.Unlock()

	var buf bytes.Buffer
	for i, input := range task.Inputs {
		name := input.Name()
		location, hasValue := l.GetShardLocation(name)
		if !hasValue {
			panic("hmmm, we just checked all inputs are registered!")
		}
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(name)
		buf.WriteString("@")
		buf.WriteString(location.URL())
	}
	return buf.String()
}