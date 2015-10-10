package agent

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"sync"

	"github.com/chrislusf/glow/driver/cmd"
	"github.com/chrislusf/glow/io/store"
	"github.com/chrislusf/glow/resource"
	"github.com/chrislusf/glow/resource/service_discovery/client"
	"github.com/chrislusf/glow/util"
	"github.com/golang/protobuf/proto"
)

type LiveDataStore struct {
	store           store.DataStore
	killHeartBeater chan bool
}

func NewLiveDataStore(s store.DataStore) *LiveDataStore {
	return &LiveDataStore{
		store:           s,
		killHeartBeater: make(chan bool, 1),
	}
}

func (ds *LiveDataStore) Destroy() {
	ds.killHeartBeater <- true
	ds.store.Destroy()
}

type AgentServerOption struct {
	Leader      *string
	Port        *int
	Dir         *string
	DataCenter  *string
	Rack        *string
	MaxExecutor *int
	MemoryMB    *int64
	CPULevel    *int
}

type AgentServer struct {
	Option            *AgentServerOption
	leader            string
	Port              int
	name2Store        map[string]*LiveDataStore
	dir               string
	name2StoreLock    sync.Mutex
	wg                sync.WaitGroup
	l                 net.Listener
	computeResource   *resource.ComputeResource
	allocatedResource *resource.ComputeResource
}

func NewAgentServer(option *AgentServerOption) *AgentServer {
	as := &AgentServer{
		Option:     option,
		leader:     *option.Leader,
		Port:       *option.Port,
		dir:        *option.Dir,
		name2Store: make(map[string]*LiveDataStore),
		computeResource: &resource.ComputeResource{
			CPUCount: *option.MaxExecutor,
			CPULevel: *option.CPULevel,
			MemoryMB: *option.MemoryMB,
		},
		allocatedResource: &resource.ComputeResource{},
	}

	err := as.Init()
	if err != nil {
		panic(err)
	}

	return as
}

// Start starts to listen on a port, returning the listening port
// r.Port can be pre-set or leave it as zero
// The actual port set to r.Port
func (r *AgentServer) Init() (err error) {
	r.l, err = net.Listen("tcp", ":"+strconv.Itoa(r.Port))
	if err != nil {
		log.Fatal(err)
	}

	r.Port = r.l.Addr().(*net.TCPAddr).Port
	fmt.Println("AgentServer starts on:", r.Port)
	return
}

func (as *AgentServer) Run() {
	//register agent
	killHeartBeaterChan := make(chan bool, 1)
	go client.NewHeartBeater(as.Port, as.leader).StartAgentHeartBeat(killHeartBeaterChan, func(values url.Values) {
		resource.AddToValues(values, as.computeResource, as.allocatedResource)
		values.Add("dataCenter", *as.Option.DataCenter)
		values.Add("rack", *as.Option.Rack)
	})

	for {
		// Listen for an incoming connection.
		conn, err := as.l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		as.wg.Add(1)
		go func() {
			defer as.wg.Done()
			defer conn.Close()
			as.handleRequest(conn)
		}()
	}
}

func (r *AgentServer) Stop() {
	r.l.Close()
	r.wg.Wait()
}

// Handles incoming requests.
func (r *AgentServer) handleRequest(conn net.Conn) {

	buf := make([]byte, 4)

	f, message, err := util.ReadBytes(conn, buf)
	if f != util.Data {
		//strange if this happens
		println("read", len(message.Bytes()), "request flag:", f, "data", string(message.Data()))
		return
	}

	if err != nil {
		log.Printf("Failed to read command %s:%v", string(message.Data()), err)
	}
	if bytes.HasPrefix(message.Data(), []byte("PUT ")) {
		name := string(message.Data()[4:])
		r.handleWriteConnection(conn, name)
	} else if bytes.HasPrefix(message.Data(), []byte("GET ")) {
		name := string(message.Data()[4:])
		offset := util.ReadUint64(conn)
		r.handleLocalReadConnection(conn, name, int64(offset))
	} else if bytes.HasPrefix(message.Data(), []byte("CMD ")) {
		newCmd := &cmd.ControlMessage{}
		err := proto.Unmarshal(message.Data()[4:], newCmd)
		if err != nil {
			log.Fatal("unmarshaling error: ", err)
		}
		reply := r.handleCommandConnection(conn, newCmd)
		if reply != nil {
			data, err := proto.Marshal(reply)
			if err != nil {
				log.Fatal("marshaling error: ", err)
			}
			conn.Write(data)
		}
	}

}
