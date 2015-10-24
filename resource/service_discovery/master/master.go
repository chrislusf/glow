// master collects data from agents, and manage named network channels.
package master

import (
	"net/http"
	"sync"

	"github.com/chrislusf/glow/util"
)

type TeamMaster struct {
	channels     map[string][]*ChannelInformation
	channelsLock sync.Mutex

	MasterResource *MasterResource
}

func (tl *TeamMaster) statusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = "0.001"
	util.Json(w, r, http.StatusOK, infos)
}

func RunMaster(listenOn string) {
	tl := &TeamMaster{}
	tl.channels = make(map[string][]*ChannelInformation)
	tl.MasterResource = NewMasterResource()

	http.HandleFunc("/", tl.statusHandler)
	http.HandleFunc("/agent/assign", tl.requestAgentHandler)
	http.HandleFunc("/agent/update", tl.updateAgentHandler)
	http.HandleFunc("/agent/", tl.listAgentsHandler)
	http.HandleFunc("/channel/", tl.handleChannel)

	http.ListenAndServe(listenOn, nil)

}
