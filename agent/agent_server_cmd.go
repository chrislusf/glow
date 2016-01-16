package agent

import (
	"net"
	"strings"

	"github.com/chrislusf/glow/driver/cmd"
)

func (as *AgentServer) handleCommandConnection(conn net.Conn,
	command *cmd.ControlMessage) *cmd.ControlMessage {
	reply := &cmd.ControlMessage{}
	if command.GetType() == cmd.ControlMessage_StartRequest {
		reply.Type = cmd.ControlMessage_StartResponse.Enum()
		remoteAddress := conn.RemoteAddr().String()
		// println("remote address is", remoteAddress)
		host := remoteAddress[:strings.LastIndex(remoteAddress, ":")]
		command.StartRequest.Host = &host
		reply.StartResponse = as.handleStart(conn, command.StartRequest)
	}
	if command.GetType() == cmd.ControlMessage_DeleteDatasetShardRequest {
		reply.Type = cmd.ControlMessage_DeleteDatasetShardResponse.Enum()
		reply.DeleteDatasetShardResponse = as.handleDeleteDatasetShard(conn, command.DeleteDatasetShardRequest)
	}
	if command.GetType() == cmd.ControlMessage_GetStatusRequest {
		reply.Type = cmd.ControlMessage_GetStatusResponse.Enum()
		reply.GetStatusResponse = as.handleStatus(command.GetStatusRequest)
	}
	// TODO: skip return reply for now
	return nil
}
