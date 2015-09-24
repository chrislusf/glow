package rsync

import (
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

type RsyncServer struct {
	fileLocation string
	Port         int
}

func NewRsyncServer(file string) *RsyncServer {
	return &RsyncServer{
		fileLocation: file,
	}
}

func (rs *RsyncServer) handler(w http.ResponseWriter, req *http.Request) {
	file, err := os.Open(rs.fileLocation)
	if err != nil {
		log.Printf("Failed to open %s: %v", rs.fileLocation, err)
		return
	}
	defer file.Close()
	http.ServeContent(w, req, "", time.Now(), file)
}

// go start a http server locally that will respond predictably to ranged requests
func (rs *RsyncServer) Start() {
	s := http.NewServeMux()
	s.HandleFunc("/content", rs.handler)

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}

	rs.Port = listener.Addr().(*net.TCPAddr).Port

	go func() {
		http.Serve(listener, s)
	}()
}
