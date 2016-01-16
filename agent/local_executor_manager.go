package agent

import (
	"sync"
	"time"
)

type LocalExecutorManager struct {
	sync.Mutex
	id2ExecutorStatus map[int32]*ExecutorStatus
}

func newLocalExecutorsManager() *LocalExecutorManager {
	m := &LocalExecutorManager{
		id2ExecutorStatus: make(map[int32]*ExecutorStatus),
	}
	go m.purgeExpiredEntries()
	return m
}

func (m *LocalExecutorManager) getExecutorStatus(id int32) *ExecutorStatus {
	m.Lock()
	defer m.Unlock()
	executorStatus, ok := m.id2ExecutorStatus[id]
	if ok {
		return executorStatus
	}

	executorStatus = &ExecutorStatus{LastAccessTime: time.Now()}
	m.id2ExecutorStatus[id] = executorStatus

	return executorStatus
}

// purge executor status older than 24 hours to save memory
func (m *LocalExecutorManager) purgeExpiredEntries() {
	for {
		func() {
			m.Lock()
			cutoverLimit := time.Now().Add(-24 * time.Hour)
			for id, executorStatus := range m.id2ExecutorStatus {
				if executorStatus.LastAccessTime.Before(cutoverLimit) {
					delete(m.id2ExecutorStatus, id)
				}
			}
			m.Unlock()
			time.Sleep(1 * time.Hour)
		}()
	}
}
