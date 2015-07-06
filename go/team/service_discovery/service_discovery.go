package service_discovery

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/chrislusf/glow/go/team/service"
	"github.com/chrislusf/glow/go/util"
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
		for _, p := range ret {
			locations = append(locations, p.Input.String())
		}
	}
	return locations
}
