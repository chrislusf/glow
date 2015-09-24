package client

import (
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/glow/util"
)

type HeartBeater struct {
	Leaders      []string
	Name         string
	ServicePort  int
	SleepSeconds int64
}

func NewHeartBeater(name string, servicePort int, leader string) *HeartBeater {
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	h := &HeartBeater{
		Leaders:      []string{leader},
		Name:         name,
		ServicePort:  servicePort,
		SleepSeconds: 10,
	}
	return h
}

// Starts heart beating
func (h *HeartBeater) StartHeartBeat(killChan chan bool) {
	for {
		h.beat()
		select {
		case <-killChan:
			return
		default:
			time.Sleep(time.Duration(rand.Int63n(h.SleepSeconds/2)+h.SleepSeconds/2) * time.Second)
		}
	}
}

func (h *HeartBeater) beat() {
	for _, leader := range h.Leaders {
		values := make(url.Values)
		values.Add("servicePort", strconv.Itoa(h.ServicePort))
		util.Post("http://"+leader+"/join"+h.Name, values)
	}
}
