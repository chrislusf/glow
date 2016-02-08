// network channel that sends data
package sender

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/chrislusf/glow/util"
)

// Talk with local agent
func NewDirectSendChannel(tlsConfig *tls.Config, name string, target string, wg *sync.WaitGroup) (chan []byte, error) {

	var readerWriter io.ReadWriteCloser

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

	if tlsConfig != nil {
		readerWriter = tls.Client(conn, tlsConfig)
	} else {
		readerWriter = conn
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer readerWriter.Close()
		buf := make([]byte, 4)
		util.WriteBytes(readerWriter, buf, util.NewMessage(util.Data, []byte("PUT "+name)))

		for data := range ch {
			util.WriteBytes(readerWriter, buf, util.NewMessage(util.Data, data))
		}

		util.WriteBytes(readerWriter, buf, util.NewMessage(util.CloseChannel, nil))
	}()

	return ch, nil
}

func NewSendChannel(tlsConfig *tls.Config, name string, port int, wg *sync.WaitGroup) (chan []byte, error) {
	return NewDirectSendChannel(tlsConfig, name, "localhost:"+strconv.Itoa(port), wg)
}
