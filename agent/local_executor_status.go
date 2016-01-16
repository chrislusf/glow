package agent

import (
	"time"
)

type ExecutorStatus struct {
	RequestHash    int32
	RequestTime    time.Time
	InputLength    int
	OutputLength   int
	ReadyTime      time.Time
	RunTime        time.Time
	StopTime       time.Time
	LastAccessTime time.Time // used for expiring entries
}

func (es *ExecutorStatus) IsClosed() bool {
	return !es.StopTime.IsZero()
}
