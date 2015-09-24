package leader

import ()

const TimeOutLimit = 25 // seconds

type Path string
type ServiceLocation string

func (p *Path) String() string {
	return string(*p)
}
func (i *ServiceLocation) String() string {
	return string(*i)
}

type RemoteService struct {
	ServiceLocation ServiceLocation
	LastHeartBeat   int64 // epoc seconds
}

func (rp *RemoteService) String() string {
	return rp.ServiceLocation.String()
}
