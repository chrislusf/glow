package driver

import (
	"flag"

	"github.com/chrislusf/glow/flow"
)

type TaskOption struct {
	ContextId          int
	TaskGroupId        int
	FistTaskName       string
	Inputs             string
	ExecutableFileHash string
	ChannelBufferSize  int
}

var taskOption TaskOption

func init() {
	flag.IntVar(&taskOption.ContextId, "glow.flow.id", -1, "flow id")
	flag.IntVar(&taskOption.TaskGroupId, "glow.taskGroup.id", -1, "task group id")
	flag.StringVar(&taskOption.FistTaskName, "glow.task.name", "", "name of first task in the task group")
	flag.StringVar(&taskOption.Inputs, "glow.taskGroup.inputs", "", "comma and @ seperated input locations")
	flag.StringVar(&taskOption.ExecutableFileHash, "glow.exe.hash", "", "hash of executable binary file")
	flag.IntVar(&taskOption.ChannelBufferSize, "glow.channel.bufferSize", 0, "channel buffer size for reading inputs")

	flow.RegisterTaskRunner(NewTaskRunner(&taskOption))
}
