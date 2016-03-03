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
