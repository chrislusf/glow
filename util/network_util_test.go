package util

import (
	"crypto/rand"
	"crypto/tls"
	"log"
	"net"
	"testing"
)

func acceptAndWrite(listener net.Listener, text string, t *testing.T) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			t.Error(err)
			break
		}
		defer conn.Close()
		if n, err := conn.Write([]byte(text)); n != len(text) || err != nil {
			t.Errorf("Wrote %d bytes, error: %v", n, err)
		}
	}
}

func TestDialWithTlsConfig(t *testing.T) {
	cert, err := tls.LoadX509KeyPair("test_certs/server.pem", "test_certs/server.key")
	if err != nil {
		log.Fatalf("server: loadkeys: %s", err)
	}

	config := tls.Config{
		Certificates: []tls.Certificate{cert},
		Rand:         rand.Reader,
	}

	listener, err := tls.Listen("tcp", ":8000", &config)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go acceptAndWrite(listener, "abc", t)

	config.InsecureSkipVerify = true
	conn, err := Dial(&config, "127.0.0.1:8000")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	reply := make([]byte, 3)
	if n, err := conn.Read(reply); n != 3 || err != nil {
		t.Errorf("Read %d bytes, error: %v", n, err)
	}
}

func TestDialWithoutTlsConfig(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:8008")
	if err != nil || listener == nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go acceptAndWrite(listener, "abc", t)

	conn, err := Dial(nil, ":8008")
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	reply := make([]byte, 3)
	if n, err := conn.Read(reply); n != 3 || err != nil {
		t.Errorf("Read %d bytes, error: %v", n, err)
	}
}
