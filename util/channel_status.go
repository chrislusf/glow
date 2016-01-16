package util

import (
	"time"
)

type ChannelStatus struct {
	Length    int64
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
	s.Length += int64(delta)
}

func (s *ChannelStatus) ReportClose() {
	s.StopTime = time.Now()
}
