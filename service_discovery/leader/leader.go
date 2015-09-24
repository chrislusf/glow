// Leader acts as a transient team leader
// It register each service's active locations.
package leader

import (
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo"
)

type TeamLeader struct {
	machines map[Path][]*RemoteService
}

func (tl *TeamLeader) statusHandler(c *echo.Context) error {
	infos := make(map[string]interface{})
	infos["Version"] = "0.001"
	// utils.WriteJson(c, http.StatusOK, infos)
	c.String(http.StatusOK, "Hello World")
	return nil
}

func (tl *TeamLeader) listHandler(c *echo.Context) error {
	path := Path(c.P(0))

	freshRps := make([]*RemoteService, 0)
	rps, ok := tl.machines[path]
	if !ok {
		c.JSON(http.StatusOK, freshRps)
		return nil
	}
	now := time.Now().Unix()
	for _, rp := range rps {
		if rp.LastHeartBeat+TimeOutLimit >= now {
			freshRps = append(freshRps, rp)
		}
	}
	for i, j := 0, len(freshRps)-1; i < j; i, j = i+1, j-1 {
		freshRps[i], freshRps[j] = freshRps[j], freshRps[i]
	}
	tl.machines[path] = freshRps
	c.JSON(http.StatusOK, freshRps)
	return nil
}

func (tl *TeamLeader) joinHandler(c *echo.Context) error {
	servicePort := c.Request().FormValue("servicePort")
	host := c.Request().Host
	if strings.Contains(host, ":") {
		host = host[0:strings.Index(host, ":")]
	}
	location := ServiceLocation(host + ":" + servicePort)
	path := Path(c.P(0))
	// println(path, ":", location)

	rps, ok := tl.machines[path]
	if !ok {
		rps = make([]*RemoteService, 0)
	}
	found := false
	for _, rp := range rps {
		if rp.ServiceLocation == location {
			rp.LastHeartBeat = time.Now().Unix()
			found = true
			break
		}
	}
	if !found {
		rps = append(rps, &RemoteService{
			ServiceLocation: location,
			LastHeartBeat:   time.Now().Unix(),
		})
	}
	tl.machines[path] = rps

	c.JSON(http.StatusAccepted, tl.machines)

	// id, _ := strconv.Atoi(c.Param("url"))
	// utils.WriteJson(c, http.StatusOK, infos)
	// c.String(http.StatusOK, c.P(0)+" runs at "+location.String()+".")
	return nil
}

func RunLeader(listenOn string) {
	tl := &TeamLeader{}
	tl.machines = make(map[Path][]*RemoteService)

	e := echo.New()
	e.Get("/", tl.statusHandler)
	e.Post("/join/*", tl.joinHandler)
	e.Get("/list/*", tl.listHandler)
	e.Run(listenOn)
}
