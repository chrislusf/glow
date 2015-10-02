package agent

import (
	"log"
	"net"
	"os/exec"

	"github.com/chrislusf/glow/driver/cmd"
	"github.com/golang/protobuf/proto"
)

func (as *AgentServer) handleCommandConnection(conn net.Conn,
	command *cmd.ControlMessage) *cmd.ControlMessage {
	reply := &cmd.ControlMessage{}
	if command.GetType() == cmd.ControlMessage_StartRequest {
		reply.Type = cmd.ControlMessage_StartResponse.Enum()
		reply.StartResponse = as.handleStart(conn, command.StartRequest)
	}
	if command.GetType() == cmd.ControlMessage_DeleteDatasetShardRequest {
		reply.Type = cmd.ControlMessage_DeleteDatasetShardResponse.Enum()
		reply.DeleteDatasetShardResponse = as.handleDeleteDatasetShard(conn, command.DeleteDatasetShardRequest)
	}
	return reply
}

func (as *AgentServer) handleStart(conn net.Conn,
	startRequest *cmd.StartRequest) *cmd.StartResponse {
	reply := &cmd.StartResponse{}

	// println("received command:", *startRequest.Path)

	cmd := exec.Command(
		*startRequest.Path,
		startRequest.Args...,
	)
	cmd.Env = startRequest.Envs
	cmd.Dir = *startRequest.Dir
	cmd.Stdout = conn
	cmd.Stderr = conn
	err := cmd.Start()
	if err != nil {
		log.Printf("Failed to start command %s under %s: %v",
			cmd.Path, cmd.Dir, err)
		*reply.Error = err.Error()
	} else {
		reply.Pid = proto.Int32(int32(cmd.Process.Pid))
	}

	cmd.Wait()

	return nil
}

func (as *AgentServer) handleDeleteDatasetShard(conn net.Conn,
	deleteRequest *cmd.DeleteDatasetShardRequest) *cmd.DeleteDatasetShardResponse {

	as.handleDelete(*deleteRequest.Name)

	return nil
}
