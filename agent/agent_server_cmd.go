package agent

import (
	"net"
	"strings"

	"github.com/chrislusf/glow/driver/cmd"
)

func (as *AgentServer) handleCommandConnection(conn net.Conn,
	command *cmd.ControlMessage) *cmd.ControlMessage {
	reply := &cmd.ControlMessage{}
	switch command.GetType() {
	case cmd.ControlMessage_StartRequest:
		reply.Type = cmd.ControlMessage_StartResponse.Enum()
		remoteAddress := conn.RemoteAddr().String()
		// println("remote address is", remoteAddress)
		host := remoteAddress[:strings.LastIndex(remoteAddress, ":")]
		command.StartRequest.Host = &host
		reply.StartResponse = as.handleStart(conn, command.StartRequest)
		return nil
	case cmd.ControlMessage_DeleteDatasetShardRequest:
		reply.Type = cmd.ControlMessage_DeleteDatasetShardResponse.Enum()
		reply.DeleteDatasetShardResponse = as.handleDeleteDatasetShard(conn, command.DeleteDatasetShardRequest)
	case cmd.ControlMessage_GetStatusRequest:
		reply.Type = cmd.ControlMessage_GetStatusResponse.Enum()
		reply.GetStatusResponse = as.handleGetStatusRequest(command.GetStatusRequest)
	case cmd.ControlMessage_StopRequest:
		reply.Type = cmd.ControlMessage_StopResponse.Enum()
		reply.StopResponse = as.handleStopRequest(command.StopRequest)
	case cmd.ControlMessage_LocalStatusReportRequest:
		reply.Type = cmd.ControlMessage_LocalStatusReportResponse.Enum()
		reply.LocalStatusReportResponse = as.handleLocalStatusReportRequest(command.LocalStatusReportRequest)
	}
	return reply
}
