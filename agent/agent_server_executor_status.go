package agent

import (
	"github.com/chrislusf/glow/driver/cmd"
	"github.com/golang/protobuf/proto"
)

func (as *AgentServer) handleStatus(getStatusRequest *cmd.GetStatusRequest) *cmd.GetStatusResponse {
	requestId := getStatusRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

	reply := &cmd.GetStatusResponse{
		StartRequestHash: proto.Int32(requestId),
		RequestTime:      proto.Int64(stat.RequestTime.Unix()),
		StartTime:        proto.Int64(stat.StartTime.Unix()),
		StopTime:         proto.Int64(stat.StopTime.Unix()),
	}

	return reply
}

func (as *AgentServer) handleStopRequest(stopRequest *cmd.StopRequest) *cmd.StopResponse {
	requestId := stopRequest.GetStartRequestHash()
	stat := as.localExecutorManager.getExecutorStatus(requestId)

	if stat.Process != nil {
		stat.Process.Kill()
		stat.Process = nil
	}

	reply := &cmd.StopResponse{
		StartRequestHash: proto.Int32(requestId),
	}

	return reply
}
