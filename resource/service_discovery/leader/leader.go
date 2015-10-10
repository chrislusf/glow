// Leader acts as a transient team leader
// It register each service's active locations.
package leader

import (
	"net/http"
	"sync"

	"github.com/chrislusf/glow/util"
)

type TeamLeader struct {
	channels     map[string][]*ChannelInformation
	channelsLock sync.Mutex

	LeaderResource *LeaderResource
}

func (tl *TeamLeader) statusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = "0.001"
	util.Json(w, r, http.StatusOK, infos)
}

func RunLeader(listenOn string) {
	tl := &TeamLeader{}
	tl.channels = make(map[string][]*ChannelInformation)
	tl.LeaderResource = NewLeaderResource()

	http.HandleFunc("/", tl.statusHandler)
	http.HandleFunc("/agent/assign", tl.requestAgentHandler)
	http.HandleFunc("/agent/update", tl.updateAgentHandler)
	http.HandleFunc("/agent/", tl.listAgentsHandler)
	http.HandleFunc("/channel/", tl.handleChannel)

	http.ListenAndServe(listenOn, nil)

}
