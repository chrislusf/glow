// receiver
package receiver

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
)

type Receiver struct {
	Port    int
	Handler func(io.Reader, io.WriteCloser)
	wg      sync.WaitGroup

	l net.Listener
}

func NewReceiver() *Receiver {
	return &Receiver{}
}

// Start starts to listen on a port, returning the listening port
// r.Port can be pre-set or leave it as zero
// The actual port set to r.Port
func (r *Receiver) Init() (err error) {
	r.l, err = net.Listen("tcp", ":"+strconv.Itoa(r.Port))
	if err != nil {
		log.Fatal(err)
	}

	r.Port = r.l.Addr().(*net.TCPAddr).Port
	fmt.Println("Hello World from receiver:", r.Port)
	return
}

func (r *Receiver) Loop() {
	for {
		// Listen for an incoming connection.
		conn, err := r.l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		r.wg.Add(1)
		go r.handleRequest(conn)
	}
}

func (r *Receiver) Stop() {
	r.l.Close()
	r.wg.Wait()
}

// Handles incoming requests.
func (r *Receiver) handleRequest(conn net.Conn) {
	defer r.wg.Done()

	in := io.Reader(conn)
	out := io.WriteCloser(conn)
	r.Handler(in, out)

	// Close the connection when you're done with it.
	out.Close()
	conn.Close()
}
