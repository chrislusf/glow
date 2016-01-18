package driver

import (
	"flag"

	"github.com/chrislusf/glow/flow"
)

type TaskOption struct {
	ContextId          int
	TaskGroupId        int
	FistTaskName       string // only for debugging purpose, show it in command line
	Inputs             string
	ExecutableFileHash string
	ChannelBufferSize  int
	RequestId          uint64
}

var taskOption TaskOption

func init() {
	flag.IntVar(&taskOption.ContextId, "glow.flow.id", -1, "flow id")
	flag.IntVar(&taskOption.TaskGroupId, "glow.taskGroup.id", -1, "task group id")
	flag.StringVar(&taskOption.FistTaskName, "glow.task.name", "", "name of first task in the task group")
	flag.StringVar(&taskOption.Inputs, "glow.taskGroup.inputs", "", "comma and @ seperated input locations")
	flag.StringVar(&taskOption.ExecutableFileHash, "glow.exe.hash", "", "hash of executable binary file")
	flag.IntVar(&taskOption.ChannelBufferSize, "glow.channel.bufferSize", 0, "channel buffer size for reading inputs")
	flag.Uint64Var(&taskOption.RequestId, "glow.request.id", 0, "request id received from agent")

	flow.RegisterTaskRunner(NewTaskRunner(&taskOption))
}
