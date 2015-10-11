package driver

import (
	"flag"
	"sync"

	"github.com/chrislusf/glow/io/receiver"
	"github.com/chrislusf/glow/io/sender"
)

var ()

type NetworkContext struct {
	AgentPort int
}

var networkContext NetworkContext

func init() {
	flag.IntVar(&networkContext.AgentPort, "glow.agent.port", 8931, "agent port")
}

func GetSendChannel(name string, wg *sync.WaitGroup) (chan []byte, error) {
	return sender.NewChannel(name, networkContext.AgentPort, wg)
}

func GetDirectReadChannel(name, location string) (chan []byte, error) {
	rc := receiver.NewReceiveChannel(name, 0)
	return rc.GetDirectChannel(location)
}
