// network channel that sends data
package sender

import (
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/chrislusf/glow/util"
	"github.com/golang/snappy"
)

// Talk with local agent
func NewDirectSendChannel(name string, target string, wg *sync.WaitGroup) (chan []byte, error) {

	ch := make(chan []byte)

	// connect to a TCP server
	network := "tcp"
	raddr, err := net.ResolveTCPAddr(network, target)
	if err != nil {
		return ch, fmt.Errorf("Fail to resolve %s: %v", target, err)
	}

	conn, err := net.DialTCP(network, nil, raddr)
	if err != nil {
		return ch, fmt.Errorf("Fail to dial send %s: %v", raddr, err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer conn.Close()
		buf := make([]byte, 4)
		util.WriteBytes(conn, buf, util.NewMessage(util.Data, []byte("PUT "+name)))

		for data := range ch {
			util.WriteBytes(conn, buf, util.NewMessage(util.Data, snappy.Encode(nil, data)))
		}

		util.WriteBytes(conn, buf, util.NewMessage(util.CloseChannel, nil))
	}()

	return ch, nil
}

func NewSendChannel(name string, port int, wg *sync.WaitGroup) (chan []byte, error) {
	return NewDirectSendChannel(name, "localhost:"+strconv.Itoa(port), wg)
}
