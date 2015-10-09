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
}

func (s *DatasetShard) Name() string {
	return fmt.Sprintf("ct-%d-ds-%d-shard-%d", s.Parent.context.Id, s.Parent.Id, s.Id)
}

func (shard *DatasetShard) SetupReadingChans() {
	for _, task := range shard.ReadingTasks {
		for i, s := range task.Inputs {
			if s == shard {
				shard.readingChans = append(shard.readingChans, task.InputChans[i])
			}
		}
	}
	// fmt.Printf("shard %s has reading channel #: %d\n", shard.Name(), len(shard.readingChans))
}

func (s *DatasetShard) SendForRead(t reflect.Value) {
	for _, c := range s.readingChans {
		c <- t
	}
}

func (s *DatasetShard) CloseRead() {
	for _, c := range s.readingChans {
		close(c)
	}
}
