// Agent
package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/chrislusf/glow/go/resource"
	"github.com/labstack/echo"
)

// Agent has multiple responsibilities
// 1. accept commands to start/stop processes, and report back process info
// 2. ...
type Agent struct {
	processes map[resource.RoleName]resource.Process
}

func (a *Agent) statusHandler(c *echo.Context) error {
	infos := make(map[string]interface{})
	infos["Version"] = "0.001"
	// utils.WriteJson(c, http.StatusOK, infos)
	c.String(http.StatusOK, "Hello World")
	return nil
}

func (a *Agent) joinHandler(c *echo.Context) error {
	p := c.Request().FormValue("process")

	// id, _ := strconv.Atoi(c.Param("url"))
	// utils.WriteJson(c, http.StatusOK, infos)
	c.String(http.StatusOK, c.P(0)+" runs at "+p+".")
	return nil
}

var (
	listenOn = flag.String("listen", ":24368", "Port to listen on")
)

func main() {
	fmt.Println("Hello From Agent!")
	a := &Agent{}
	a.processes = make(map[resource.RoleName]resource.Process)

	e := echo.New()
	e.Get("/", a.statusHandler)
	e.Post("/join/*", a.joinHandler)
	e.Run(*listenOn)
}
