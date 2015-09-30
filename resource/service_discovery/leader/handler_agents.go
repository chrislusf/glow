// Leader acts as a transient team leader
// It register each service's active locations.
package leader

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/chrislusf/glow/resource"
	"github.com/labstack/echo"
)

func (tl *TeamLeader) listAgentsHandler(c *echo.Context) error {
	c.JSON(http.StatusAccepted, tl.LeaderResource.Topology.Resource)
	return nil
}

func (tl *TeamLeader) requestAgentHandler(c *echo.Context) error {
	requestBlob := []byte(c.Request().FormValue("request"))
	var request resource.AllocationRequest
	err := json.Unmarshal(requestBlob, &request)
	if err != nil {
		return fmt.Errorf("request JSON unmarshal error:%v, json:%s", err, string(requestBlob))
	}

	fmt.Printf("request:\n%+v\n", request)

	result := tl.allocate(&request)
	fmt.Printf("result: %v\n%+v\n", result.Error, result.Allocations)
	if result.Error != "" {
		c.JSON(http.StatusNotFound, result)
	}

	c.JSON(http.StatusAccepted, result)

	return nil
}

func (tl *TeamLeader) updateAgentHandler(c *echo.Context) error {
	servicePortString := c.Request().FormValue("servicePort")
	servicePort, err := strconv.Atoi(servicePortString)
	if err != nil {
		log.Printf("Strange: servicePort not found: %s, %v", servicePortString, err)
	}
	host := c.Request().Host
	if strings.Contains(host, ":") {
		host = host[0:strings.Index(host, ":")]
	}
	// println("received agent update from", host+":"+servicePort)
	ai := &resource.AgentInformation{
		Location: resource.Location{
			DataCenter: c.Request().FormValue("dataCenter"),
			Rack:       c.Request().FormValue("rack"),
			Server:     host,
			Port:       servicePort,
		},
		Resource: resource.NewComputeResourceFromRequest(c.Request()),
	}

	tl.LeaderResource.UpdateAgentInformation(ai)

	c.NoContent(http.StatusAccepted)

	return nil
}
