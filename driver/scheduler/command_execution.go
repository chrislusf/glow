package scheduler

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"time"

	"github.com/chrislusf/glow/driver/cmd"
	"github.com/chrislusf/glow/resource"
	"github.com/chrislusf/glow/resource/service_discovery/client"
	"github.com/chrislusf/glow/util"
	"github.com/golang/protobuf/proto"
)

func NewStartRequest(path string, dir string, args []string, allocated resource.ComputeResource, envs []string, host string, port int32) *cmd.ControlMessage {
	request := &cmd.ControlMessage{
		Type: cmd.ControlMessage_StartRequest.Enum(),
		StartRequest: &cmd.StartRequest{
			Path: proto.String(path),
			Args: args,
			Dir:  proto.String(dir),
			Resource: &cmd.ComputeResource{
				CpuCount: proto.Int32(int32(allocated.CPUCount)),
				CpuLevel: proto.Int32(int32(allocated.CPULevel)),
				Memory:   proto.Int32(int32(allocated.MemoryMB)),
			},
			Envs:     envs,
			Host:     proto.String(host),
			Port:     proto.Int32(port),
			HashCode: proto.Uint32(0),
		},
	}

	// generate a unique hash code for the request
	data, err := proto.Marshal(request)
	if err != nil {
		log.Fatalf("marshaling start request error: %v", err)
		return nil
	}
	request.StartRequest.HashCode = proto.Uint32(uint32(util.Hash(data)))

	return request
}

func NewGetStatusRequest(requestId uint32) *cmd.ControlMessage {
	return &cmd.ControlMessage{
		Type: cmd.ControlMessage_GetStatusRequest.Enum(),
		GetStatusRequest: &cmd.GetStatusRequest{
			StartRequestHash: proto.Uint32(requestId),
		},
	}
}

func NewStopRequest(requestId uint32) *cmd.ControlMessage {
	return &cmd.ControlMessage{
		Type: cmd.ControlMessage_StopRequest.Enum(),
		StopRequest: &cmd.StopRequest{
			StartRequestHash: proto.Uint32(requestId),
		},
	}
}

func NewDeleteDatasetShardRequest(name string) *cmd.ControlMessage {
	return &cmd.ControlMessage{
		Type: cmd.ControlMessage_DeleteDatasetShardRequest.Enum(),
		DeleteDatasetShardRequest: &cmd.DeleteDatasetShardRequest{
			Name: proto.String(name),
		},
	}
}

func RemoteDirectExecute(tlsConfig *tls.Config, server string, command *cmd.ControlMessage) error {
	conn, err := getDirectCommandConnection(tlsConfig, server)
	if err != nil {
		return err
	}
	defer conn.Close()

	return doExecute(server, conn, command)
}

// doExecute() sends a request and expects the output from the connection
func doExecute(server string, conn io.ReadWriteCloser, command *cmd.ControlMessage) error {

	buf := make([]byte, 4)

	// serialize the commend
	data, err := proto.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshaling execute request error: %v", err)
	}

	// send the command
	if err = util.WriteData(conn, buf, []byte("CMD "), data); err != nil {
		return fmt.Errorf("failed to write to %s: %v", server, err)
	}

	// println("command sent")

	// read output and print it to stdout
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		fmt.Printf("%s>%s\n", server, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("Failed to scan output: %v", err)
	}

	return err
}

func RemoteDirectCommand(tlsConfig *tls.Config, server string, command *cmd.ControlMessage) (response *cmd.ControlMessage, err error) {
	conn, err := getDirectCommandConnection(tlsConfig, server)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return doCommand(server, conn, command)
}

// doCommand() sends a request and expects a response object
func doCommand(server string, conn io.ReadWriteCloser, command *cmd.ControlMessage) (response *cmd.ControlMessage, err error) {

	buf := make([]byte, 4)

	// serialize the commend
	data, err := proto.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("marshaling command error: %v", err)
	}

	// send the command
	if err = util.WriteData(conn, buf, []byte("CMD "), data); err != nil {
		return nil, fmt.Errorf("failed to write command to %s: %v", server, err)
	}

	// println("command sent")

	// read response
	replyBytes, err := ioutil.ReadAll(conn)
	if err != nil {
		return nil, fmt.Errorf("cmd response: %v", err)
	}

	// unmarshal the bytes
	response = &cmd.ControlMessage{}
	err = proto.Unmarshal(replyBytes, response)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling error: %v", err)
	}

	return response, err
}

func getCommandConnection(tlsConfig *tls.Config, leader string, agentName string) (io.ReadWriteCloser, error) {
	l := client.NewNameServiceProxy(leader)

	// looking for the agentName
	var target string
	for {
		locations := l.Find(agentName)
		if len(locations) > 0 {
			target = locations[0]
		}
		if target != "" {
			break
		} else {
			time.Sleep(time.Second)
			print("z")
		}
	}

	return getDirectCommandConnection(tlsConfig, target)
}

func getDirectCommandConnection(tlsConfig *tls.Config, target string) (io.ReadWriteCloser, error) {
	return util.Dial(tlsConfig, target)
}
