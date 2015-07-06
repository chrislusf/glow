package service_discovery

import (
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/chrislusf/glow/go/util"
)

type HeartBeater struct {
	Leaders      []string
	Name         string
	Location     string
	SleepSeconds int64
}

func NewHeartBeater(name string, location string, leader string) *HeartBeater {
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	h := &HeartBeater{
		Leaders:      []string{leader},
		Name:         name,
		Location:     location,
		SleepSeconds: 10,
	}
	return h
}

// Starts heart beating
func (h *HeartBeater) Start() {
	for {
		h.beat()
		time.Sleep(time.Duration(rand.Int63n(h.SleepSeconds/2)+h.SleepSeconds/2) * time.Second)
	}
}

func (h *HeartBeater) beat() {
	for _, leader := range h.Leaders {
		values := make(url.Values)
		values.Add("input", h.Location)
		util.Post("http://"+leader+"/join"+h.Name, values)
	}
}
