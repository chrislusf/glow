package sender

import (
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/chrislusf/glow/util"
)

// Talk with local agent
func NewChannel(name string, port int, wg *sync.WaitGroup) (chan []byte, error) {
	ch := make(chan []byte)

	// connect to a TCP server
	network := "tcp"
	target := "localhost:" + strconv.Itoa(port)
	raddr, err := net.ResolveTCPAddr(network, target)
	if err != nil {
		return ch, fmt.Errorf("Fail to resolve %s: %v", target, err)
	}

	conn, err := net.DialTCP(network, nil, raddr)
	if err != nil {
		return ch, fmt.Errorf("Fail to dial %s: %v", raddr, err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer conn.Close()
		buf := make([]byte, 4)
		util.WriteBytes(conn, buf, util.NewMessage(util.Data, []byte("PUT "+name)))
		for data := range ch {
			util.WriteBytes(conn, buf, util.NewMessage(util.Data, data))
		}
		util.WriteBytes(conn, buf, util.NewMessage(util.CloseChannel, nil))
	}()

	return ch, nil
}
