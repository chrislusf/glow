package flow

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/chrislusf/glow/io"
)

func (d *Dataset) GetShards() []*DatasetShard {
	return d.Shards
}

type Dataset struct {
	Id           int
	context      *FlowContext
	Type         reflect.Type
	Shards       []*DatasetShard
	Step         *Step
	ReadingSteps []*Step
	OutputChans  []reflect.Value
}

func NewDataset(context *FlowContext, t reflect.Type) *Dataset {
	d := &Dataset{
		Id:      len(context.Datasets),
		context: context,
		Type:    t,
	}
	context.Datasets = append(context.Datasets, d)
	return d
}

func (d *Dataset) RunSelf(stepId int) {
	var wg sync.WaitGroup
	for _, shard := range d.Shards {
		wg.Add(1)
		go func(shard *DatasetShard) {
			defer wg.Done()
			shard.SetupReadingChans()

			// start to run
			var t reflect.Value
			for ok := true; ok; {
				if t, ok = shard.WriteChan.Recv(); ok {
					shard.SendForRead(t)
					d.sendToOutputChans(t)
				}
			}
			shard.CloseRead()
		}(shard)
	}
	wg.Wait()
	d.closeOutputChans()
	// println("dataset", stepId, "stopped")
	return
}

func (d *Dataset) Run() {
	d.context.Run()
}

func (d *Dataset) sendToOutputChans(t reflect.Value) {
	for _, ch := range d.OutputChans {
		elemType := ch.Type().Elem()
		t = io.CleanObject(t, d.Type, elemType)
		ch.Send(t)
	}
}

func (d *Dataset) closeOutputChans() {
	for _, ch := range d.OutputChans {
		ch.Close()
	}
}

func assertChannelOf(ch interface{}, dsType reflect.Type) {
	chType := reflect.TypeOf(ch)
	if chType.Kind() != reflect.Chan {
		panic(fmt.Sprintf("%v should be a channel", ch))
	}
	if chType.Elem() == dsType {
		return
	}
	if chType.Elem().Kind() == reflect.Struct {
		return
	}
	panic(fmt.Sprintf("chan %s should have element type %s", chType, dsType))
}

func (d *Dataset) AddOutput(ch interface{}) *Dataset {
	assertChannelOf(ch, d.Type)
	d.OutputChans = append(d.OutputChans, reflect.Indirect(reflect.ValueOf(ch)))
	return d
}
