// network channel that receives data
package receiver

import (
	"io"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/chrislusf/glow/resource/service_discovery/client"
	"github.com/chrislusf/glow/util"
)

type ReceiveChannel struct {
	Ch     chan []byte
	offset uint64
	name   string
}

func FindTarget(name string, leader string) (target string) {
	l := client.NewNameServiceProxy(leader)
	for {
		locations := l.Find(name)
		if len(locations) > 0 {
			target = locations[rand.Intn(len(locations))]
		}
		if target != "" {
			break
		} else {
			time.Sleep(time.Second)
			// print("z")
		}
	}
	return
}

func NewReceiveChannel(name string, offset uint64) *ReceiveChannel {
	return &ReceiveChannel{
		name:   name,
		offset: offset,
	}
}

// Not thread safe
func (rc *ReceiveChannel) GetDirectChannel(target string) (chan []byte, error) {
	if rc.Ch != nil {
		return rc.Ch, nil
	}
	rc.Ch = make(chan []byte)
	go func() {
		rc.receiveTopicFrom(target)
	}()

	return rc.Ch, nil
}

func (rc *ReceiveChannel) receiveTopicFrom(target string) {
	// connect to a TCP server
	network := "tcp"
	raddr, err := net.ResolveTCPAddr(network, target)
	if err != nil {
		log.Printf("Fail to resolve %s:%v", target, err)
		return
	}

	// println("dial tcp", raddr.String())
	conn, err := net.DialTCP(network, nil, raddr)
	if err != nil {
		log.Printf("Fail to dial %s:%v", raddr, err)
		time.Sleep(time.Second)
		return
	}
	defer conn.Close()

	buf := make([]byte, 4)

	util.WriteBytes(conn, buf, util.NewMessage(util.Data, []byte("GET "+rc.name)))

	util.WriteUint64(conn, rc.offset)

	util.WriteBytes(conn, buf, util.NewMessage(util.Data, []byte("ok")))

	ticker := time.NewTicker(time.Millisecond * 1100)
	defer ticker.Stop()
	go func() {
		buf := make([]byte, 4)
		for range ticker.C {
			util.WriteBytes(conn, buf, util.NewMessage(util.Data, []byte("ok")))
			// print(".")
		}
	}()

	for {
		f, data, err := util.ReadBytes(conn, buf)
		if err == io.EOF {
			// print("recieve close chan2: eof")
			break
		}
		if err != nil {
			log.Printf("receive error:%v", err)
			continue
		}
		rc.offset += 4 + 1
		if f != util.Data {
			// print("recieve close chan1: ", string([]byte{byte(f)}))
			break
		}
		// println("receive raw data :", string(data.Bytes()))
		rc.offset += uint64(len(data.Data()))
		rc.Ch <- data.Data()
	}
	close(rc.Ch)
}
