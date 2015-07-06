// resourcer manage team members
package resource

import ()

type RegionName string
type Region struct {
	Name string `json:"name,omitempty"`
}

type MachineName string
type Machine struct {
	Region     RegionName  `json:"region,omitempty"`
	Name       MachineName `json:"name,omitempty"`
	Processes  []Process   `json:"processes,omitempty"`
	StatusPort Port        `json:"statusPort,omitempty"`
}

type Port int

type Process struct {
	Host Machine `json:"host,omitempty"`
	Port Port    `json:"port,omitempty"`
}

type RoleName string
