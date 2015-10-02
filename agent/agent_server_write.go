package agent

import (
	"fmt"
	"io"
	"log"

	"github.com/chrislusf/glow/agent/store"
	"github.com/chrislusf/glow/resource/service_discovery/client"
	"github.com/chrislusf/glow/util"
)

func (as *AgentServer) handleWriteConnection(r io.Reader, name string) {
	as.name2StoreLock.Lock()
	ds, ok := as.name2Store[name]
	if !ok {
		s, err := store.NewLocalFileDataStore(as.dir, fmt.Sprintf("%s-%d", name, as.Port))
		if err != nil {
			log.Printf("Failed to create a queue on disk: %v", err)
			as.name2StoreLock.Unlock()
			return
		}
		as.name2Store[name] = NewLiveDataStore(s)
		ds = as.name2Store[name]

	}
	//register stream
	go client.NewHeartBeater(as.Port, "localhost:8930").StartChannelHeartBeat(ds.killHeartBeater, name)

	as.name2StoreLock.Unlock()

	buf := make([]byte, 4)
	for {
		_, message, err := util.ReadBytes(r, buf)
		if err == io.EOF {
			// println("agent recv eof:", string(message.Bytes()))
			break
		}
		if err == nil {
			util.WriteBytes(ds.store, buf, message)
			// println("agent recv:", string(message.Bytes()))
		}
		if message.Flag() != util.Data {
			// println("finished writing", name)
			break
		}
	}
}

func (as *AgentServer) handleDelete(name string) {
	as.name2StoreLock.Lock()
	ds, ok := as.name2Store[name]
	if !ok {
		return
	}

	delete(as.name2Store, name)
	as.name2StoreLock.Unlock()

	ds.Destroy()
}
