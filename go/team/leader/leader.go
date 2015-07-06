// Leader
package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/chrislusf/glow/go/team/service"
	"github.com/labstack/echo"
)

type TeamLeader struct {
	machines map[service.Path][]*service.RemoteService
}

func (tl *TeamLeader) statusHandler(c *echo.Context) error {
	infos := make(map[string]interface{})
	infos["Version"] = "0.001"
	// utils.WriteJson(c, http.StatusOK, infos)
	c.String(http.StatusOK, "Hello World")
	return nil
}

func (tl *TeamLeader) listHandler(c *echo.Context) error {
	path := service.Path(c.P(0))

	freshRps := make([]*service.RemoteService, 0)
	rps, ok := tl.machines[path]
	if !ok {
		c.JSON(http.StatusOK, freshRps)
		return nil
	}
	now := time.Now().Unix()
	for _, rp := range rps {
		if rp.LastHeartBeat+service.TimeOutLimit >= now {
			freshRps = append(freshRps, rp)
		}
	}
	tl.machines[path] = freshRps
	c.JSON(http.StatusOK, freshRps)
	return nil
}

func (tl *TeamLeader) joinHandler(c *echo.Context) error {
	input := service.Input(c.Request().FormValue("input"))
	path := service.Path(c.P(0))

	rps, ok := tl.machines[path]
	if !ok {
		rps = make([]*service.RemoteService, 0)
	}
	found := false
	for _, rp := range rps {
		if rp.Input == input {
			rp.LastHeartBeat = time.Now().Unix()
			found = true
			break
		}
	}
	if !found {
		rps = append(rps, &service.RemoteService{
			Input:         input,
			LastHeartBeat: time.Now().Unix(),
		})
	}
	tl.machines[path] = rps

	c.JSON(http.StatusAccepted, tl.machines)

	// id, _ := strconv.Atoi(c.Param("url"))
	// utils.WriteJson(c, http.StatusOK, infos)
	// c.String(http.StatusOK, c.P(0)+" runs at "+input.String()+".")
	return nil
}

var (
	listenOn = flag.String("listen", ":8930", "Port to listen on")
)

func main() {
	fmt.Println("Hello From Team Leader!")
	tl := &TeamLeader{}
	tl.machines = make(map[service.Path][]*service.RemoteService)

	e := echo.New()
	e.Get("/", tl.statusHandler)
	e.Post("/join/*", tl.joinHandler)
	e.Get("/list/*", tl.listHandler)
	e.Run(*listenOn)
}
