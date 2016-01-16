package agent

import (
	"os"
	"time"
)

type ExecutorStatus struct {
	RequestHash    int32
	RequestTime    time.Time
	InputLength    int
	OutputLength   int
	StartTime      time.Time
	StopTime       time.Time
	Process        *os.Process
	LastAccessTime time.Time // used for expiring entries
}

func (es *ExecutorStatus) IsClosed() bool {
	return !es.StopTime.IsZero()
}
