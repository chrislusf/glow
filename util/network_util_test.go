package util

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"testing"
)

// TODO(yaxiongzhao): These 2 tests are flaky. Sometimes fail with:
// --- FAIL: TestDialWithTlsConfig (0.02s)
// network_util_test.go:15: accept tcp [::]:8000: use of closed network connection

func acceptAndWrite(listener net.Listener, text string) {
	conn, err := listener.Accept()
	if err != nil {
		panic(fmt.Sprint(err))
	}
	defer conn.Close()

	if n, err := conn.Write([]byte(text)); n != len(text) || err != nil {
		panic(fmt.Sprintf("Wrote %d bytes, error: %v", n, err))
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
	addr := listener.Addr().String()
	defer listener.Close()

	go acceptAndWrite(listener, "abc")

	clientConfig := tls.Config{
		Certificates:       []tls.Certificate{cert},
		Rand:               rand.Reader,
		InsecureSkipVerify: true,
	}
	conn, err := Dial(&clientConfig, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	reply := make([]byte, 3)
	n, err := conn.Read(reply)
	if n != 3 {
		t.Errorf("Read %d bytes, error: %v", n, err)
	}
}

func TestDialWithoutTlsConfig(t *testing.T) {
	listener, err := net.Listen("tcp", ":8008")
	if err != nil || listener == nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	defer listener.Close()

	go acceptAndWrite(listener, "abc")

	conn, err := Dial(nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	reply := make([]byte, 3)
	n, err := conn.Read(reply)
	if n != 3 {
		t.Errorf("Read %d bytes, error: %v", n, err)
	}
}
