// Package netchan creates network channels. The network channels are managed by
// glow agent.
package netchan

import (
	"crypto/tls"
	"log"
	"reflect"
	"sync"

	"github.com/chrislusf/glow/netchan/receiver"
	"github.com/chrislusf/glow/netchan/sender"
	"github.com/chrislusf/glow/util"
)

func GetDirectReadChannel(tlsConfig *tls.Config, name, location string, chanBufferSize int) (chan []byte, error) {
	rc := receiver.NewReceiveChannel(tlsConfig, name, 0)
	return rc.GetDirectChannel(location, chanBufferSize)
}

func GetDirectSendChannel(tlsConfig *tls.Config, name string, location string, wg *sync.WaitGroup) (chan []byte, error) {
	return sender.NewDirectSendChannel(tlsConfig, name, location, wg)
}

func ConnectRawReadChannelToTyped(c chan []byte, out chan reflect.Value, t reflect.Type, wg *sync.WaitGroup) (status *util.ChannelStatus) {
	status = util.NewChannelStatus()
	wg.Add(1)
	go func() {
		defer wg.Done()
		status.ReportStart()

		for data := range c {
			decodedData, err := DecodeData(data, t)
			if err != nil {
				log.Fatal("Read from raw channel:", err)
			} else {
				out <- decodedData
				status.ReportAdd(1)
			}
		}

		close(out)
		status.ReportClose()
	}()
	return status
}

func ConnectTypedWriteChannelToRaw(writeChan reflect.Value, c chan []byte, wg *sync.WaitGroup) (status *util.ChannelStatus) {
	status = util.NewChannelStatus()
	wg.Add(1)
	go func() {
		defer wg.Done()
		status.ReportStart()

		var t reflect.Value
		for ok := true; ok; {
			if t, ok = writeChan.Recv(); ok {
				data, err := EncodeData(t)
				if err != nil {
					log.Fatal("Write to raw channel:", err)
				}
				c <- data
				status.ReportAdd(1)
			}
		}
		close(c)
		status.ReportClose()
	}()
	return status
}
