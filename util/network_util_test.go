package util

import (
	"crypto/rand"
	"crypto/tls"
	"log"
	"net"
	"testing"
)

// TODO(yaxiongzhao): These 2 tests are flaky. Sometimes fail with:
// --- FAIL: TestDialWithTlsConfig (0.02s)
// network_util_test.go:15: accept tcp [::]:8000: use of closed network connection

func acceptAndWrite(listener net.Listener, text string, t *testing.T) {
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if n, err := conn.Write([]byte(text)); n != len(text) || err != nil {
		t.Errorf("Wrote %d bytes, error: %v", n, err)
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

	listener, err := tls.Listen("tcp", ":0", &config)
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	defer listener.Close()

	go acceptAndWrite(listener, "abc", t)

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
	if n, err := conn.Read(reply); n != 3 || err != nil {
		t.Errorf("Read %d bytes, error: %v", n, err)
	}
}

func TestDialWithoutTlsConfig(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil || listener == nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	defer listener.Close()

	go acceptAndWrite(listener, "abc", t)

	conn, err := Dial(nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	reply := make([]byte, 3)
	if n, err := conn.Read(reply); n != 3 || err != nil {
		t.Errorf("Read %d bytes, error: %v", n, err)
	}
}
