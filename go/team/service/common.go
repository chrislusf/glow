package service

import ()

const TimeOutLimit = 25 // seconds

type Path string
type Input string

func (p *Path) String() string {
	return string(*p)
}
func (i *Input) String() string {
	return string(*i)
}

type RemoteService struct {
	Input         Input
	LastHeartBeat int64 // epoc seconds
}

func (rp *RemoteService) String() string {
	return rp.Input.String()
}
