package agent

import (
	"io"

	"github.com/chrislusf/glow/util"
)

func (as *AgentServer) handleWriteConnection(r io.Reader, name string) {

	dsStore := as.storageBackend.CreateNamedDatasetShard(name)

	// println(name, "start writing.")

	buf := make([]byte, 4)
	for {
		_, message, err := util.ReadBytes(r, buf)
		if err == io.EOF {
			// println("agent recv eof:", string(message.Bytes()))
			break
		}
		if err == nil {
			util.WriteBytes(dsStore, buf, message)
			// println("agent recv:", string(message.Bytes()))
		}
		if message.Flag() != util.Data {
			// println("finished writing", name)
			break
		}
	}
}

func (as *AgentServer) handleDelete(name string) {

	as.storageBackend.DeleteNamedDatasetShard(name)

}
