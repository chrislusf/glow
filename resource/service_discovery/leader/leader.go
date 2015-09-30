// Leader acts as a transient team leader
// It register each service's active locations.
package leader

import (
	"net/http"

	"github.com/labstack/echo"
)

type TeamLeader struct {
	channels map[string][]*ChannelInformation

	LeaderResource *LeaderResource
}

func (tl *TeamLeader) statusHandler(c *echo.Context) error {
	infos := make(map[string]interface{})
	infos["Version"] = "0.001"
	// utils.WriteJson(c, http.StatusOK, infos)
	c.String(http.StatusOK, "Hello World")
	return nil
}

func RunLeader(listenOn string) {
	tl := &TeamLeader{}
	tl.channels = make(map[string][]*ChannelInformation)
	tl.LeaderResource = NewLeaderResource()

	e := echo.New()
	e.Get("/", tl.statusHandler)
	e.Post("/agent/assign", tl.requestAgentHandler)
	e.Post("/agent/update", tl.updateAgentHandler)
	e.Get("/agent/*", tl.listAgentsHandler)
	e.Post("/channel/*", tl.updateChannelHandler)
	e.Get("/channel/*", tl.listChannelsHandler)
	e.Run(listenOn)
}
