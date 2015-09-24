package util

import ()

type ControlFlag byte

const (
	Data         ControlFlag = ControlFlag('D')
	CloseChannel             = ControlFlag('C')
	FullStop                 = ControlFlag('S')
)
