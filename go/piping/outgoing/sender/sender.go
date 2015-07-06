// sender
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	_ "time"

	"github.com/chrislusf/glow/go/message"
	"github.com/chrislusf/glow/go/team/service_discovery"
)

var (
	name          = flag.String("name", "worker", "a service name")
	leader        = flag.String("leader", "localhost:8930", "leader managing services")
	host          = flag.String("host", "localhost", "server name or ip")
	lineDelimiter = flag.Bool("line", true, "separate input by lines")

	target = flag.String("target", "", "bytes flow to this destination if set")
)

func main() {
	flag.Parse()

	if *target == "" {
		leader := service_discovery.NewNameServiceAgent(*leader)
		locations := leader.Find(*name)
		if len(locations) > 0 {
			*target = locations[0]
		}
	}

	// connect to a TCP server
	network := "tcp"
	raddr, err := net.ResolveTCPAddr(network, *target)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.DialTCP(network, nil, raddr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("Connected to ", *target)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		count, err := io.Copy(os.Stdout, conn)
		if err != nil {
			if err == io.EOF {
				log.Println("response closed.")
				conn.CloseRead()
				return
			}
			log.Fatal(err)
		}
		log.Println("output read", count)
	}()

	// buf := make([]byte, 64*1024)

	// client sends data to the server
	wg.Add(1)
	go func() {
		defer wg.Done()
		if *lineDelimiter {
			scanner := bufio.NewScanner(os.Stdin)
			m := &message.Message{
				Type: message.DataType,
			}
			count := 0
			for scanner.Scan() {
				m.Data = scanner.Bytes()
				if err := m.Write(conn); err != nil {
					fmt.Fprintln(os.Stderr, "write to conn:", err)
					break
				}
				count++
			}
			if err := scanner.Err(); err != nil {
				fmt.Fprintln(os.Stderr, "reading standard input:", err)
			}
			log.Printf("copied %d lines", count)
		} else {
			count, err := io.Copy(conn, os.Stdin)
			if err != nil {
				// if the client reached its retry limit, give up
				if err == io.EOF {
					log.Println("input is closed")
					return
				}
				// not a GAS error, just panic
				log.Fatal(err)
			}
			log.Printf("copied %d", count)

		}
		conn.CloseWrite()
	}()

	wg.Wait()
}
