package client

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	service "github.com/chrislusf/glow/service_discovery/leader"
	"github.com/chrislusf/glow/util"
)

type NameServiceAgent struct {
	Leaders []string
}

func NewNameServiceAgent(leaders ...string) *NameServiceAgent {
	n := &NameServiceAgent{
		Leaders: leaders,
	}
	return n
}

func (n *NameServiceAgent) Find(name string) (locations []string) {
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	for _, l := range n.Leaders {
		jsonBlob, err := util.Get("http://" + l + "/list" + name)
		if err != nil {
			log.Printf("Failed to list from %s:%v", l, err)
		}
		var ret []service.RemoteService
		err = json.Unmarshal(jsonBlob, &ret)
		if err != nil {
			fmt.Printf("%s/list%s unmarshal error:%v, json:%s", l, name, err, string(jsonBlob))
			continue
		}
		if len(ret) <= 0 {
			return nil
		}
		for _, rs := range ret {
			locations = append(locations, rs.ServiceLocation.String())
		}
	}
	return locations
}
