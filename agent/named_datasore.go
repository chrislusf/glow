package agent

import (
	"fmt"
	"log"
	"sync"

	"github.com/chrislusf/glow/netchan/store"
)

type ManagedDatasetShards struct {
	dir            string
	port           int
	name2Store     map[string]store.DataStore
	name2StoreCond *sync.Cond
}

func NewManagedDatasetShards(dir string, port int) *ManagedDatasetShards {
	var lock sync.Mutex
	return &ManagedDatasetShards{
		dir:            dir,
		port:           port,
		name2Store:     make(map[string]store.DataStore),
		name2StoreCond: sync.NewCond(&lock),
	}
}

func (m *ManagedDatasetShards) doDelete(name string) {

	ds, ok := m.name2Store[name]
	if !ok {
		return
	}

	delete(m.name2Store, name)

	ds.Destroy()
}

func (m *ManagedDatasetShards) DeleteNamedDatasetShard(name string) {

	m.name2StoreCond.L.Lock()
	defer m.name2StoreCond.L.Unlock()

	m.doDelete(name)

}

func (m *ManagedDatasetShards) CreateNamedDatasetShard(name string) store.DataStore {

	m.name2StoreCond.L.Lock()
	_, ok := m.name2Store[name]
	if ok {
		m.doDelete(name)
	}

	s, err := store.NewLocalFileDataStore(m.dir, fmt.Sprintf("%s-%d", name, m.port))
	if err != nil {
		log.Printf("Failed to create a queue on disk: %v", err)
		m.name2StoreCond.L.Unlock()
		return nil
	}

	m.name2Store[name] = s
	// println(name, "is broadcasting...")
	m.name2StoreCond.Broadcast()

	m.name2StoreCond.L.Unlock()

	return s

}

func (m *ManagedDatasetShards) WaitForNamedDatasetShard(name string) store.DataStore {

	var ds store.DataStore
	var ok bool

	m.name2StoreCond.L.Lock()
	for {
		ds, ok = m.name2Store[name]
		if ok {
			break
		}
		println(name, "is waiting to read...")
		m.name2StoreCond.Wait()
	}
	m.name2StoreCond.L.Unlock()

	return ds

}
