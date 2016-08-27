package flow

import (
	"reflect"
	"sync"

	"github.com/chrislusf/glow/netchan"
)

func (d *Dataset) RunDatasetInStandAloneMode() {
	var wg sync.WaitGroup

	if len(d.ExternalInputChans) > 0 {
		d.connectExternalInputChansToRead(&wg)
		for _, shard := range d.Shards {
			shard.SetupReadingChans()
		}
	} else {
		for _, shard := range d.Shards {
			wg.Add(1)
			go func(shard *DatasetShard) {
				defer wg.Done()
				// println("setup shard reading chans", shard.Name())
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
				// println("close shard reading", shard.Name())
				shard.CloseRead()
			}(shard)
		}
	}

	wg.Wait()
	d.closeExternalOutputChans()
	return
}

func (d *Dataset) Run() {
	d.context.Run()
}

func (d *Dataset) connectExternalInputChansToRead(wg *sync.WaitGroup) {
	for _, ch := range d.ExternalInputChans {
		wg.Add(1)
		go func(inputChan reflect.Value) {
			defer wg.Done()
			var t reflect.Value
			for ok := true; ok; {
				if t, ok = inputChan.Recv(); ok {
					for _, shard := range d.Shards {
						shard.SendForRead(t)
					}
				}
			}
			for _, shard := range d.Shards {
				shard.CloseRead()
			}
		}(ch)
	}
}

func (d *Dataset) sendToExternalOutputChans(t reflect.Value) {
	for _, ch := range d.ExternalOutputChans {
		elemType := ch.Type().Elem()
		t = netchan.CleanObject(t, d.Type, elemType)
		ch.Send(t)
	}
}

func (d *Dataset) closeExternalOutputChans() {
	for _, ch := range d.ExternalOutputChans {
		ch.Close()
	}
}
