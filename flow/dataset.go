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
	Id                  int
	context             *FlowContext
	Type                reflect.Type
	Shards              []*DatasetShard
	Step                *Step
	ReadingSteps        []*Step
	ExternalInputChans  []reflect.Value
	ExternalOutputChans []reflect.Value
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
					// hookup output channels
					d.sendToExternalOutputChans(t)
				}
			}
			shard.CloseRead()
		}(shard)
	}
	wg.Wait()
	d.closeExternalOutputChans()
	// println("dataset", stepId, "stopped")
	return
}

func (d *Dataset) Run() {
	d.context.Run()
}

func (d *Dataset) sendToExternalOutputChans(t reflect.Value) {
	for _, ch := range d.ExternalOutputChans {
		elemType := ch.Type().Elem()
		t = io.CleanObject(t, d.Type, elemType)
		ch.Send(t)
	}
}

func (d *Dataset) closeExternalOutputChans() {
	for _, ch := range d.ExternalOutputChans {
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
	d.ExternalOutputChans = append(d.ExternalOutputChans, reflect.Indirect(reflect.ValueOf(ch)))
	return d
}
