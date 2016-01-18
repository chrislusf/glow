package agent

import (
	"os"
	"time"

	"github.com/chrislusf/glow/util"
)

type AgentExecutorStatus struct {
	util.ExecutorStatus
	RequestHash    int32
	Process        *os.Process
	LastAccessTime time.Time // used for expiring entries
}
