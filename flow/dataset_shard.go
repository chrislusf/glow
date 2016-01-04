package flow

import (
	"fmt"
	"reflect"
)

type DatasetShard struct {
	Id           int
	Parent       *Dataset
	WriteChan    reflect.Value
	ReadingTasks []*Task

	readingChans []chan reflect.Value
	counter      int
	closed       bool
}

func (d *Dataset) SetupShard(n int) {
	ctype := reflect.ChanOf(reflect.BothDir, d.Type)
	for i := 0; i < n; i++ {
		ds := &DatasetShard{
			Id:        i,
			Parent:    d,
			WriteChan: reflect.MakeChan(ctype, 0),
		}
		// println("created shard", ds.Name())
		d.Shards = append(d.Shards, ds)
	}
}

func (s *DatasetShard) Name() string {
	return fmt.Sprintf("ct-%d-ds-%d-shard-%d", s.Parent.context.Id, s.Parent.Id, s.Id)
}

func (shard *DatasetShard) SetupReadingChans() {
	// get unique list of tasks since ReadingTasks can have duplicates
	// especially when one dataset is used twice in a task, e.g. selfJoin()
	var uniqTasks []*Task
	seenTasks := make(map[*Task]bool)
	for _, task := range shard.ReadingTasks {
		if ok := seenTasks[task]; ok {
			continue
		}
		seenTasks[task] = true
		uniqTasks = append(uniqTasks, task)
	}

	for _, task := range uniqTasks {
		for i, s := range task.Inputs {
			if s == shard {
				shard.readingChans = append(shard.readingChans, task.InputChans[i])
			}
		}
	}
	// fmt.Printf("shard %s has reading tasks:%d channel:%d\n", shard.Name(), len(shard.ReadingTasks), len(shard.readingChans))
}

func (s *DatasetShard) SendForRead(t reflect.Value) {
	s.counter++
	for _, c := range s.readingChans {
		// println(s.Name(), "send chan", i, "entry:", s.counter)
		c <- t
	}
}

func (s *DatasetShard) CloseRead() {
	for _, c := range s.readingChans {
		close(c)
	}
	s.closed = true
}
