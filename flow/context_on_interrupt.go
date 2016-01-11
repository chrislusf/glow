package flow

import (
	"fmt"
)

func (fc *FlowContext) OnInterrupt() {

	for _, step := range fc.Steps {
		if step.Output != nil {
			fmt.Printf("step:%s%d\n", step.Name, step.Id)
			for _, input := range step.Inputs {
				fmt.Printf("  input :d%d\n", input.Id)
				for _, shard := range input.Shards {
					if shard.closed {
						fmt.Printf("     shard:%d completed %d\n", shard.Id, shard.counter)
					} else {
						fmt.Printf("     shard:%d processed %d\n", shard.Id, shard.counter)
					}
				}
			}
			fmt.Printf("  output:d%d\n", step.Output.Id)
			for _, task := range step.Tasks {
				for _, shard := range task.Outputs {
					if shard.closed {
						fmt.Printf("     shard:%d completed %d\n", shard.Id, shard.counter)
					} else {
						fmt.Printf("     shard:%d processed %d\n", shard.Id, shard.counter)
					}
				}
			}
		}
	}
}
