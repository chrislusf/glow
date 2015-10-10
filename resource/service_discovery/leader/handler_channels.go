// Leader acts as a transient team leader
// It register each service's active locations.
package leader

import (
	"net/http"
	"strings"
	"time"

	"github.com/chrislusf/glow/util"
)

type ChannelInformation struct {
	Location      string
	LastHeartBeat time.Time
}

func (tl *TeamLeader) handleChannel(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		tl.updateChannelHandler(w, r)
	} else {
		tl.listChannelsHandler(w, r)
	}
}

func (tl *TeamLeader) listChannelsHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/channel/"):]

	freshChannels := make([]*ChannelInformation, 0)
	rps, ok := tl.channels[path]
	if !ok {
		util.Json(w, r, http.StatusOK, freshChannels)
		return
	}
	for _, rp := range rps {
		if rp.LastHeartBeat.Add(TimeOutLimit * time.Second).After(time.Now()) {
			freshChannels = append(freshChannels, rp)
		}
	}
	for i, j := 0, len(freshChannels)-1; i < j; i, j = i+1, j-1 {
		freshChannels[i], freshChannels[j] = freshChannels[j], freshChannels[i]
	}
	tl.channelsLock.Lock()
	tl.channels[path] = freshChannels
	tl.channelsLock.Unlock()
	util.Json(w, r, http.StatusOK, freshChannels)
}

// put agent information list under a path
func (tl *TeamLeader) updateChannelHandler(w http.ResponseWriter, r *http.Request) {
	servicePort := r.FormValue("servicePort")
	host := r.Host
	if strings.Contains(host, ":") {
		host = host[0:strings.Index(host, ":")]
	}
	location := host + ":" + servicePort
	path := r.URL.Path[len("/channel/"):]
	// println(path, ":", location)

	rps, ok := tl.channels[path]
	if !ok {
		rps = make([]*ChannelInformation, 0)
	}
	found := false
	for _, rp := range rps {
		if rp.Location == location {
			rp.LastHeartBeat = time.Now()
			found = true
			break
		}
	}
	if !found {
		rps = append(rps, &ChannelInformation{
			Location:      location,
			LastHeartBeat: time.Now(),
		})
	}
	tl.channelsLock.Lock()
	tl.channels[path] = rps
	tl.channelsLock.Unlock()

	util.Json(w, r, http.StatusAccepted, tl.channels)

}
