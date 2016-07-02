package util

import (
	"crypto/tls"
	"testing"
)

func dummy(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return &tls.Certificate{}, nil
}

func TestDial(t *testing.T) {
	tlsConfig := &tls.Config{}
	tlsConfig.GetCertificate = dummy
	listener, err := tls.Listen("tcp", "localhost:9999", tlsConfig)
	if err != nil {
		t.Error(err)
	}
	go listener.Accept()
	Dial(tlsConfig, "localhost:9999")
}
