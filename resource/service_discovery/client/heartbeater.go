package client

import (
	"math/rand"
	"net/url"
	"strconv"
	"time"

	"github.com/chrislusf/glow/util"
)

type HeartBeater struct {
	Leaders      []string
	ServicePort  int
	SleepSeconds int64
}

func NewHeartBeater(localPort int, leader string) *HeartBeater {
	h := &HeartBeater{
		Leaders:      []string{leader},
		ServicePort:  localPort,
		SleepSeconds: 10,
	}
	return h
}

func (h *HeartBeater) StartChannelHeartBeat(killChan chan bool, chanName string) {
	for {
		h.beat(func(values url.Values) string {
			return "/channel/" + chanName
		})
		select {
		case <-killChan:
			return
		default:
			time.Sleep(time.Duration(rand.Int63n(h.SleepSeconds/2)+h.SleepSeconds/2) * time.Second)
		}
	}
}

// Starts heart beating
func (h *HeartBeater) StartAgentHeartBeat(killChan chan bool, fn func(url.Values)) {
	for {
		h.beat(func(values url.Values) string {
			fn(values)
			return "/agent/update"
		})
		select {
		case <-killChan:
			return
		default:
			time.Sleep(time.Duration(rand.Int63n(h.SleepSeconds/2)+h.SleepSeconds/2) * time.Second)
		}
	}
}

func (h *HeartBeater) beat(fn func(url.Values) string) {
	values := make(url.Values)
	beatToPath := fn(values)
	values.Add("servicePort", strconv.Itoa(h.ServicePort))
	for _, leader := range h.Leaders {
		_, err := util.Post("http://"+leader+beatToPath, values)
		// println("heart beat to", leader, beatToPath)
		if err != nil {
			println("Failed to heart beat to", leader, beatToPath)
		}
	}
}
