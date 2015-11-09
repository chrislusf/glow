// Package master collects data from agents, and manage named network
// channels.
package master

import (
	"net/http"
	"sync"

	"github.com/chrislusf/glow/util"
)

type NamedChannelMap struct {
	name2Chans map[string][]*ChannelInformation
	sync.Mutex
}

func (m *NamedChannelMap) GetChannels(name string) ([]*ChannelInformation, bool) {
	ret, ok := m.name2Chans[name]
	return ret, ok
}

func (m *NamedChannelMap) SetChannels(name string, chans []*ChannelInformation) {
	m.Lock()
	defer m.Unlock()
	m.name2Chans[name] = chans
}

type TeamMaster struct {
	channels       *NamedChannelMap
	MasterResource *MasterResource
}

func (tl *TeamMaster) statusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = "0.001"
	util.Json(w, r, http.StatusOK, infos)
}

func RunMaster(listenOn string) {
	tl := &TeamMaster{}
	tl.channels = &NamedChannelMap{name2Chans: make(map[string][]*ChannelInformation)}
	tl.MasterResource = NewMasterResource()

	http.HandleFunc("/", tl.statusHandler)
	http.HandleFunc("/agent/assign", tl.requestAgentHandler)
	http.HandleFunc("/agent/update", tl.updateAgentHandler)
	http.HandleFunc("/agent/", tl.listAgentsHandler)
	http.HandleFunc("/channel/", tl.handleChannel)

	http.ListenAndServe(listenOn, nil)

}
