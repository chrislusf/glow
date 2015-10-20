package agent

import (
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/chrislusf/glow/util"
)

func (as *AgentServer) handleLocalReadConnection(conn net.Conn, name string, offset int64) {
	var ds *LiveDataStore
	var ok bool

	as.name2StoreCond.L.Lock()
	for {
		ds, ok = as.name2Store[name]
		if ok {
			break
		}
		println(name, "is waiting to read...")
		as.name2StoreCond.Wait()
	}
	as.name2StoreCond.L.Unlock()

	// println(name, "start reading from:", offset)

	closeSignal := make(chan bool, 1)

	go func() {
		buf := make([]byte, 4)
		for false {
			// println("wait for reader heartbeat")
			conn.SetReadDeadline(time.Now().Add(2500 * time.Millisecond))
			_, _, err := util.ReadBytes(conn, buf)
			if err != nil {
				fmt.Printf("connection is closed? (%v)\n", err)
				closeSignal <- true
				close(closeSignal)
				return
			}
		}
	}()

	buf := make([]byte, 4)

	// loop for every read
	for {
		_, err := ds.store.ReadAt(buf, offset)
		if err != nil {
			// connection is closed
			if err != io.EOF {
				log.Printf("Read size from %s offset %d: %v", name, offset, err)
			}
			// println("got problem reading", name, offset, err.Error())
			return
		}

		offset += 4
		size := util.BytesToUint32(buf)

		// println("reading", name, offset, "size:", size)

		messageBytes := make([]byte, size)
		_, err = ds.store.ReadAt(messageBytes, offset)
		if err != nil {
			// connection is closed
			if err != io.EOF {
				log.Printf("Read data from %s offset %d: %v", name, offset, err)
			}
			return
		}
		offset += int64(size)

		m := util.LoadMessage(messageBytes)
		util.WriteBytes(conn, buf, m)

		if m.Flag() != util.Data {
			// println("Finished reading", name)
			break
		}
	}

}
