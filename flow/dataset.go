package flow

import (
	"fmt"
	"reflect"
	"sync"
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
		if elemType == d.Type {
			// fmt.Printf("sending1 %v to output chan\n", t)
			ch.Send(t)
		} else {
			// fmt.Printf("sending %d %v to output chan\n", elemType.NumField(), t)
			x := reflect.New(elemType).Elem()
			for i := 0; i < elemType.NumField(); i++ {
				x.Field(i).Set(reflect.Indirect(t.Index(i).Elem()))
			}
			ch.Send(x)
		}
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

func (d *Dataset) AddOutput(ch interface{}) {
	assertChannelOf(ch, d.Type)
	d.OutputChans = append(d.OutputChans, reflect.Indirect(reflect.ValueOf(ch)))
}
