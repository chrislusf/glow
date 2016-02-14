package util

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

func Dial(tlsConfig *tls.Config, target string) (ret io.ReadWriteCloser, err error) {

	_, err = net.ResolveTCPAddr("tcp", target)
	if err != nil {
		return nil, fmt.Errorf("Fail to resolve %s: %v", target, err)
	}

	if tlsConfig != nil {
		conn, e := tls.Dial("tcp", target, tlsConfig)
		if e != nil {
			log.Printf("Fail to tls dial %s:%v", target, e)
			time.Sleep(time.Second)
			return nil, e
		}
		ret = conn

		/*
			log.Println("client: connected to: ", conn.RemoteAddr())

			state := conn.ConnectionState()
			for _, v := range state.PeerCertificates {
				fmt.Println("subject:", v.Subject)
			}
			log.Println("client: handshake: ", state.HandshakeComplete)
			log.Println("client: mutual: ", state.NegotiatedProtocolIsMutual)
		*/

	} else {
		conn, e := net.Dial("tcp", target)
		if e != nil {
			log.Printf("Fail to net dial %s:%v", target, e)
			time.Sleep(time.Second)
			return nil, e
		}
		ret = conn
	}
	return
}
