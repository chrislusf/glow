package agent

import (
	"fmt"
	"log"
	"sync"

	"github.com/chrislusf/glow/netchan/store"
)

type ManagedDatasetShards struct {
	sync.Mutex
	dir            string
	port           int
	name2Store     map[string]store.DataStore
	name2StoreCond *sync.Cond
}

func NewManagedDatasetShards(dir string, port int) *ManagedDatasetShards {
	m := &ManagedDatasetShards{
		dir:        dir,
		port:       port,
		name2Store: make(map[string]store.DataStore),
	}
	m.name2StoreCond = sync.NewCond(m)
	return m
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

	m.Lock()
	defer m.Unlock()

	m.doDelete(name)

}

func (m *ManagedDatasetShards) CreateNamedDatasetShard(name string) store.DataStore {

	m.Lock()
	defer m.Unlock()

	_, ok := m.name2Store[name]
	if ok {
		m.doDelete(name)
	}

	s, err := store.NewLocalFileDataStore(m.dir, fmt.Sprintf("%s-%d", name, m.port))
	if err != nil {
		log.Printf("Failed to create a queue on disk: %v", err)
		return nil
	}

	m.name2Store[name] = s
	// println(name, "is broadcasting...")
	m.name2StoreCond.Broadcast()

	return s

}

func (m *ManagedDatasetShards) WaitForNamedDatasetShard(name string) store.DataStore {

	m.Lock()
	defer m.Unlock()

	for {
		if ds, ok := m.name2Store[name]; ok {
			return ds
		}
		// println(name, "is waiting to read...")
		m.name2StoreCond.Wait()
	}

	return nil

}
