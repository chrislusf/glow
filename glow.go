package main

import (
	"bufio"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/chrislusf/glow/netchan"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	a "github.com/chrislusf/glow/agent"
	r "github.com/chrislusf/glow/netchan/receiver"
	s "github.com/chrislusf/glow/netchan/sender"
	m "github.com/chrislusf/glow/resource/service_discovery/master"
	"github.com/chrislusf/glow/util"
)

var (
	app = kingpin.New("glow", "A command-line net channel.")

	master      = app.Command("master", "Start a master process")
	masterPort  = master.Flag("port", "listening port").Default("8930").Int()
	masterIp    = master.Flag("ip", "listening IP adress").Default("localhost").String()
	masterCerts = netchan.CertFiles{
		CertFile: *master.Flag("cert.file", "A PEM eoncoded certificate file").Default("").String(),
		KeyFile:  *master.Flag("key.file", "A PEM encoded private key file.").Default("").String(),
		CaFile:   *master.Flag("ca.file", "A PEM eoncoded CA's certificate file.").Default("").String(),
	}

	agent       = app.Command("agent", "Channel Agent")
	agentOption = &a.AgentServerOption{
		Dir:          agent.Flag("dir", "agent folder to store computed data").Default(os.TempDir()).String(),
		Port:         agent.Flag("port", "agent listening port").Default("8931").Int(),
		Master:       agent.Flag("master", "master address").Default("localhost:8930").String(),
		DataCenter:   agent.Flag("dataCenter", "data center name").Default("defaultDataCenter").String(),
		Rack:         agent.Flag("rack", "rack name").Default("defaultRack").String(),
		MaxExecutor:  agent.Flag("max.executors", "upper limit of executors").Default(strconv.Itoa(runtime.NumCPU())).Int(),
		CPULevel:     agent.Flag("cpu.level", "relative computing power of single cpu core").Default("1").Int(),
		MemoryMB:     agent.Flag("memory", "memory size in MB").Default("1024").Int64(),
		CleanRestart: agent.Flag("clean.restart", "clean up previous dataset files").Default("true").Bool(),
		CertFiles: netchan.CertFiles{
			CertFile: *master.Flag("cert.file", "A PEM eoncoded certificate file").Default("").String(),
			KeyFile:  *master.Flag("key.file", "A PEM encoded private key file.").Default("").String(),
			CaFile:   *master.Flag("ca.file", "A PEM eoncoded CA's certificate file.").Default("").String(),
		},
	}

	sender          = app.Command("send", "Send data to a channel")
	sendToChanName  = sender.Flag("to", "Name of a channel").Required().String()
	sendFile        = sender.Flag("file", "file to post.").ExistingFile()
	senderAgentPort = sender.Flag("port", "agent listening port").Default("8931").Int()
	senderCerts     = netchan.CertFiles{
		CertFile: *sender.Flag("cert.file", "A PEM eoncoded certificate file").Default("").String(),
		KeyFile:  *sender.Flag("key.file", "A PEM encoded private key file.").Default("").String(),
		CaFile:   *sender.Flag("ca.file", "A PEM eoncoded CA's certificate file.").Default("").String(),
	}
	// sendDelimiter  = sender.Flag("delimiter", "Verbose mode.").Short('d').String()

	receiver            = app.Command("receive", "Receive data from a channel")
	receiveFromChanName = receiver.Flag("from", "Name of a source channel").Required().String()
	receiverMaster      = receiver.Flag("master", "ip:port format").Default("localhost:8930").String()
	receiverCerts       = netchan.CertFiles{
		CertFile: *receiver.Flag("cert.file", "A PEM eoncoded certificate file").Default("").String(),
		KeyFile:  *receiver.Flag("key.file", "A PEM encoded private key file.").Default("").String(),
		CaFile:   *receiver.Flag("ca.file", "A PEM eoncoded CA's certificate file.").Default("").String(),
	}
)

func main() {
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case master.FullCommand():
		println("listening on", (*masterIp)+":"+strconv.Itoa(*masterPort))
		m.RunMaster(masterCerts.MakeTLSConfig(), (*masterIp)+":"+strconv.Itoa(*masterPort))
	case sender.FullCommand():
		tlsConfig := senderCerts.MakeTLSConfig()
		util.SetupHttpClient(tlsConfig)

		var wg sync.WaitGroup
		sendChan, err := s.NewSendChannel(tlsConfig, *sendToChanName, *senderAgentPort, &wg)
		if err != nil {
			panic(err)
		}

		file := os.Stdin
		if *sendFile != "" {
			file, err = os.Open(*sendFile)
			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()
		}

		counter := 0
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			sendChan <- scanner.Bytes()
			counter++
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
		close(sendChan)
		wg.Wait()

	case receiver.FullCommand():
		tlsConfig := receiverCerts.MakeTLSConfig()
		util.SetupHttpClient(tlsConfig)

		target := r.FindTarget(*receiveFromChanName, *receiverMaster)
		rc := r.NewReceiveChannel(tlsConfig, *receiveFromChanName, 0)
		recvChan, err := rc.GetDirectChannel(target, 128)
		if err != nil {
			panic(err)
		}
		for m := range recvChan {
			println(string(m))
		}

	case agent.FullCommand():
		agentServer := a.NewAgentServer(agentOption)
		agentServer.Run()
	}
}
