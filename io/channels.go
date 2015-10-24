// Generic network channel managed by agent.
package io

import (
	"bytes"
	"encoding/gob"
	"flag"
	"log"
	"reflect"
	"strconv"
	"sync"

	"github.com/chrislusf/glow/io/receiver"
	"github.com/chrislusf/glow/io/sender"
)

type NetworkContext struct {
	AgentPort int
}

var networkContext NetworkContext

func init() {
	flag.IntVar(&networkContext.AgentPort, "glow.agent.port", 8931, "agent port")
}

func GetLocalSendChannel(name string, wg *sync.WaitGroup) (chan []byte, error) {
	return sender.NewSendChannel(name, networkContext.AgentPort, wg)
}

func GetLocalReadChannel(name string) (chan []byte, error) {
	return GetDirectReadChannel(name, "localhost:"+strconv.Itoa(networkContext.AgentPort))
}

func GetDirectReadChannel(name, location string) (chan []byte, error) {
	rc := receiver.NewReceiveChannel(name, 0)
	return rc.GetDirectChannel(location)
}

func GetDirectSendChannel(name string, target string, wg *sync.WaitGroup) (chan []byte, error) {
	return sender.NewDirectSendChannel(name, target, wg)
}

func ConnectRawReadChannelToTyped(c chan []byte, out chan reflect.Value, t reflect.Type, wg *sync.WaitGroup) chan reflect.Value {

	wg.Add(1)
	go func() {
		defer wg.Done()

		for data := range c {
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			v := reflect.New(t)
			if err := dec.DecodeValue(v); err != nil {
				log.Fatal("data type:", v.Kind(), " decode error:", err)
			} else {
				out <- reflect.Indirect(v)
			}
		}

		close(out)
	}()

	return out

}

func ConnectTypedWriteChannelToRaw(writeChan reflect.Value, c chan []byte, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		var t reflect.Value
		for ok := true; ok; {
			if t, ok = writeChan.Recv(); ok {
				var buf bytes.Buffer
				enc := gob.NewEncoder(&buf)
				if err := enc.EncodeValue(t); err != nil {
					log.Fatal("data type:", t.Type().String(), " ", t.Kind(), " encode error:", err)
				}
				c <- buf.Bytes()
			}
		}
		close(c)

	}()

}

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
