package util

import (
	"time"
)

type ChannelStatus struct {
	Length    int
	StartTime time.Time
	StopTime  time.Time
}

func NewChannelStatus() *ChannelStatus {
	return &ChannelStatus{}
}

func (s *ChannelStatus) ReportStart() {
	s.StartTime = time.Now()
}

func (s *ChannelStatus) ReportAdd(delta int) {
	s.Length += delta
}

func (s *ChannelStatus) ReportClose() {
	s.StopTime = time.Now()
}
