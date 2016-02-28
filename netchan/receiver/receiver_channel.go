// network channel that receives data
package receiver

import (
	"crypto/tls"
	"io"
	"log"
	"math/rand"
	"time"

	"github.com/chrislusf/glow/resource/service_discovery/client"
	"github.com/chrislusf/glow/util"
)

type ReceiveChannel struct {
	Ch        chan []byte
	offset    uint64
	name      string
	tlsConfig *tls.Config
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

func NewReceiveChannel(tlsConfig *tls.Config, name string, offset uint64) *ReceiveChannel {
	return &ReceiveChannel{
		name:      name,
		offset:    offset,
		tlsConfig: tlsConfig,
	}
}

// Not thread safe
func (rc *ReceiveChannel) GetDirectChannel(target string, chanBufferSize int) (chan []byte, error) {
	if rc.Ch != nil {
		return rc.Ch, nil
	}
	rc.Ch = make(chan []byte, chanBufferSize)
	go func() {
		rc.receiveTopicFrom(target)
	}()

	return rc.Ch, nil
}

func (rc *ReceiveChannel) receiveTopicFrom(target string) {

	readWriter, err := util.Dial(rc.tlsConfig, target)
	if err != nil {
		log.Printf("Fail to dial receive %s: %v", target, err)
		return
	}
	defer readWriter.Close()

	// println("receiving", rc.name, "from", target)

	buf := make([]byte, 4)

	util.WriteBytes(readWriter, buf, util.NewMessage(util.Data, []byte("GET "+rc.name)))

	util.WriteUint64(readWriter, rc.offset)

	for {
		f, data, err := util.ReadBytes(readWriter, buf)
		if err == io.EOF {
			// print("recieve close chan2: eof")
			break
		}
		if err != nil {
			log.Printf("receive %s error:%v", rc.name, err)
			continue
		}
		rc.offset += 4 + 1
		if f != util.Data {
			// print("recieve close chan1: ", string([]byte{byte(f)}))
			util.WriteBytes(readWriter, buf, util.NewMessage(f, []byte("ack")))
			break
		}
		// println("receive raw data :", string(data.Bytes()))
		rc.offset += uint64(len(data.Data()))
		rc.Ch <- data.Data()
	}
	close(rc.Ch)
}
