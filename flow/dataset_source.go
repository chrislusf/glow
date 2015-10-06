package flow

import (
	"bufio"
	"log"
	"os"
	"reflect"
	"sync"
)

// f(chan A)
func (fc *FlowContext) Source(f interface{}, shard int) (ret *Dataset) {
	ret = fc.newNextDataset(shard, guessFunctionOutputType(f))
	step := fc.AddOneToAllStep(nil, ret)
	step.Name = "Source"
	step.Function = func(task *Task) {
		ctype := reflect.ChanOf(reflect.BothDir, ret.Type)
		outChan := reflect.MakeChan(ctype, 0)
		fn := reflect.ValueOf(f)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer outChan.Close()
			fn.Call([]reflect.Value{outChan})
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			var t reflect.Value
			i := 0
			for ok := true; ok; {
				if t, ok = outChan.Recv(); ok {
					task.Outputs[i].WriteChan.Send(t)
					i++
					if i == shard {
						i = 0
					}
				}
			}
		}()

		wg.Wait()

	}
	return
}

func (fc *FlowContext) TextFile(fname string, shard int) (ret *Dataset) {
	fn := func(out chan string) {
		file, err := os.Open(fname)
		if err != nil {
			// FIXME collect errors
			log.Panicf("Can not open file %s: %v", fname, err)
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			out <- scanner.Text()
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Scan file %s: %v", fname, err)
		}
	}
	return fc.Source(fn, shard)
}
