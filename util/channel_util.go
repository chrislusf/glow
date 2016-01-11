package util

import (
	"reflect"
	"sync"
)

func MergeChannel(cs []chan reflect.Value) (out chan reflect.Value) {
	out = make(chan reflect.Value)
	MergeChannelTo(cs, nil, out)
	return out
}

func MergeChannelTo(cs []chan reflect.Value, transformFn func(reflect.Value) reflect.Value, out chan reflect.Value) {
	var wg sync.WaitGroup

	for _, c := range cs {
		wg.Add(1)
		go func(c chan reflect.Value) {
			defer wg.Done()
			for n := range c {
				if transformFn != nil {
					n = transformFn(n)
				}
				out <- n
			}
		}(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return
}
