package main

import (
	"flag"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
	_ "time"

	"github.com/chrislusf/glow/go/piping/incoming/receiver"
	"github.com/chrislusf/glow/go/team/service_discovery"
)

func newCommand(name string, s ...string) *exec.Cmd {
	var cs []string
	cs = append(cs, s...)
	cmd := exec.Command(name, cs...)
	return cmd
}

var (
	name   = flag.String("name", "worker", "a service name")
	leader = flag.String("leader", "localhost:8930", "leader managing services")
	host   = flag.String("host", "localhost", "server name or ip")
)

// worker
// 1. heartbeat itself as role_name to leader
// 2. starts a listening receiver/collector
// 2. for each incoming stream, starts command to process it
// 3.
func main() {

	flag.Parse()

	args := flag.Args()
	r := receiver.NewReceiver()

	r.Handler = func(in io.Reader, out io.WriteCloser) {
		cmd := newCommand(args[0], args[1:]...)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			log.Panic(err)
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Panic(err)
		}
		err = cmd.Start()
		if err != nil {
			log.Panic(err)
		}

		go func() {
			_, err := io.Copy(stdin, in)
			if err != nil {
				log.Fatal(err)
			}
			// utils.IoCopy(stdin, in, "in->"+args[0])
			// io.WriteString(stdin, "this is good\n if this is a cat\n very if good\n")
			print("closing in->"+args[0], "\n")
			stdin.Close()
		}()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			teeReader := io.TeeReader(stdout, os.Stdout)
			count, err := io.Copy(out, teeReader)
			if err != nil {
				log.Fatal(err)
			}
			// utils.IoCopy(os.Stdout, stdout, args[0]+"->out")
			print("copied ", args[0], "->out ", count, "\n")
			stdout.Close()
			wg.Done()
		}()
		wg.Wait()

		err = cmd.Wait()
		if err != nil {
			log.Print(err)
		}
		print("completed ", args[0], "\n")

	}
	r.Init()

	b := service_discovery.NewHeartBeater(*name, *host+":"+strconv.Itoa(r.Port), *leader)
	go b.Start()

	r.Loop()
}
