package flow

import (
	"fmt"
	"os"
	"reflect"
	"sync"
)

func (d *Dataset) AddOutput(ch interface{}) *Dataset {
	assertChannelOf(ch, d.Type)
	d.ExternalOutputChans = append(d.ExternalOutputChans, reflect.Indirect(reflect.ValueOf(ch)))
	return d
}

func assertChannelOf(ch interface{}, dsType reflect.Type) {
	chType := reflect.TypeOf(ch)
	if chType.Kind() != reflect.Chan {
		panic(fmt.Sprintf("%v should be a channel, got: %v, want: %v", ch, chType.Kind(), reflect.Chan))
	}
	if chType.Elem() == dsType {
		return
	}
	if chType.Elem().Kind() == reflect.Struct {
		return
	}
	panic(fmt.Sprintf("chan %s should have element type %s", chType, dsType))
}

func (d *Dataset) SaveBytesToFile(fname string) {
	outChan := make(chan []byte)
	d.AddOutput(outChan)

	file, err := os.Create(fname)
	if err != nil {
		panic(fmt.Sprintf("Can not create file %s: %v", fname, err))
	}
	defer file.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := range outChan {
			file.Write(b)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		d.Run()
	}()

	wg.Wait()

}

func (d *Dataset) SaveTextToFile(fname string) {
	outChan := make(chan string)
	d.AddOutput(outChan)

	file, err := os.Create(fname)
	if err != nil {
		panic(fmt.Sprintf("Can not create file %s: %v", fname, err))
	}
	defer file.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for line := range outChan {
			file.WriteString(line)
			file.WriteString("\n")
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.Run()
	}()

	wg.Wait()
}

// collectOutput collects the output of d and returns them as a slice of the
// type specified in d.Type.
//
// Intends to be used in tests. The implementation does not optimize for performance
// or guarantee correctness under a wide range of use cases.
func collectOutput(d *Dataset) interface{} {
	outChan := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, d.Type), 0)
	d.AddOutput(outChan.Interface())

	var wg sync.WaitGroup
	wg.Add(1)

	got := reflect.MakeSlice(reflect.SliceOf(d.Type), 0, 1)
	go func() {
		defer wg.Done()
		for v, ok := outChan.Recv(); ok; v, ok = outChan.Recv() {
			got = reflect.Append(got, v)
		}
	}()

	d.Run()
	wg.Wait()

	return got.Interface()
}
