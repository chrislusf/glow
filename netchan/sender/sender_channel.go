// network channel that sends data
package sender

import (
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/chrislusf/glow/util"
)

func NewDirectSendChannel(tlsConfig *tls.Config, name string, target string, wg *sync.WaitGroup) (chan []byte, error) {

	ch := make(chan []byte)

	readerWriter, err := util.Dial(tlsConfig, target)
	if err != nil {
		return ch, fmt.Errorf("Fail to dial send %s: %v", target, err)
	}

	// println("writing to", name, "on", target)

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
