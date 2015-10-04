// Leader acts as a transient team leader
// It register each service's active locations.
package leader

import (
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo"
)

type ChannelInformation struct {
	Location      string
	LastHeartBeat time.Time
}

func (tl *TeamLeader) listChannelsHandler(c *echo.Context) error {
	path := c.P(0)

	freshChannels := make([]*ChannelInformation, 0)
	rps, ok := tl.channels[path]
	if !ok {
		c.JSON(http.StatusOK, freshChannels)
		return nil
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
	c.JSON(http.StatusOK, freshChannels)
	return nil
}

// put agent information list under a path
func (tl *TeamLeader) updateChannelHandler(c *echo.Context) error {
	servicePort := c.Request().FormValue("servicePort")
	host := c.Request().Host
	if strings.Contains(host, ":") {
		host = host[0:strings.Index(host, ":")]
	}
	location := host + ":" + servicePort
	path := c.P(0)
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

	c.JSON(http.StatusAccepted, tl.channels)

	// id, _ := strconv.Atoi(c.Param("url"))
	// utils.WriteJson(c, http.StatusOK, infos)
	// c.String(http.StatusOK, c.P(0)+" runs at "+location.String()+".")
	return nil
}
