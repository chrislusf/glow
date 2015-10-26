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
