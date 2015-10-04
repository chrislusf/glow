package flow

import (
	"reflect"
)

// map can work with multiple kinds of inputs and outputs
// 1. If 2 inputs, the first input is key, the second input is value
// 2. If 1 input, the input is value.
// 3. If last input is channel, the output goes into the channel
// 4. If 2 output, the first output is key, the second output is value
// 5. If 1 output, the output is value.
// 6. A map function may not necessarily have any output.
//
// f(A, chan B)
// input, type is same as parent Dataset's type
// output chan, element type is same as current Dataset's type
func (d *Dataset) Map(f interface{}) *Dataset {
	outType := guessFunctionOutputType(f)
	ret, step := add1ShardTo1Step(d, outType)
	step.Name = "Map"
	step.Function = func(task *Task) {
		fn := reflect.ValueOf(f)
		ft := reflect.TypeOf(f)

		var invokeMapFunc func(input reflect.Value)

		var outChan reflect.Value
		if ft.In(ft.NumIn()-1).Kind() == reflect.Chan || ft.NumOut() > 0 {
			outChan = task.Outputs[0].WriteChan
		}

		if ft.In(ft.NumIn()-1).Kind() == reflect.Chan {
			if d.Type.Kind() == reflect.Struct && ft.NumIn() != 2 {
				invokeMapFunc = func(input reflect.Value) {
					var args []reflect.Value
					for i := 0; i < input.NumField(); i++ {
						args = append(args, input.Field(i))
					}
					args = append(args, outChan)
					fn.Call(args)
				}
			} else {
				invokeMapFunc = func(input reflect.Value) {
					fn.Call([]reflect.Value{input, outChan})
				}
			}
		} else {
			if d.Type.Kind() == reflect.Struct && ft.NumIn() != 1 {
				invokeMapFunc = func(input reflect.Value) {
					var args []reflect.Value
					for i := 0; i < input.NumField(); i++ {
						args = append(args, input.Field(i))
					}
					outs := fn.Call(args)
					sendValues(outChan, outs)
				}
			} else if d.Type.Kind() == reflect.Slice && ft.NumIn() != 1 {
				invokeMapFunc = func(input reflect.Value) {
					var args []reflect.Value
					for i := 0; i < input.Len(); i++ {
						p := reflect.ValueOf(input.Index(i).Interface())
						if p.Kind() == reflect.Slice {
							// dealing with joining k, [k,v], [k,v]
							p = p.Index(1)
						}
						// println("args", i, "kind", p.Kind().String(), p.Type().String(), ":", reflect.ValueOf(p.Interface()).String())
						args = append(args, reflect.ValueOf(p.Interface()))
					}
					outs := fn.Call(args)
					sendValues(outChan, outs)
				}
			} else {
				invokeMapFunc = func(input reflect.Value) {
					outs := fn.Call([]reflect.Value{input})
					sendValues(outChan, outs)
				}
			}
		}

		for input := range task.InputChan() {
			invokeMapFunc(input)
		}
		// println("exiting d:", d.Id, "step:", step.Id, "task:", task.Id)
	}
	if ret == nil {
		d.context.Run()
	}
	return ret
}

// f(A)bool
func (d *Dataset) Filter(f interface{}) *Dataset {
	ret, step := add1ShardTo1Step(d, d.Type)
	step.Name = "Filter"
	step.Function = func(task *Task) {
		fn := reflect.ValueOf(f)
		outChan := task.Outputs[0].WriteChan
		for input := range task.InputChan() {
			outs := fn.Call([]reflect.Value{input})
			if outs[0].Bool() {
				outChan.Send(input)
			}
		}
	}
	return ret
}

func add1ShardTo1Step(d *Dataset, nextDataType reflect.Type) (ret *Dataset, step *Step) {
	ret = d.context.newNextDataset(len(d.Shards), nextDataType)
	step = d.context.AddOneToOneStep(d, ret)
	return
}

// the value over the outChan is always reflect.Value
// but the inner values are always actual interface{} object
func sendValues(outChan reflect.Value, values []reflect.Value) {
	var infs []interface{}
	for _, v := range values {
		infs = append(infs, v.Interface())
	}
	if !outChan.IsValid() {
		return
	}
	if len(infs) > 1 {
		outChan.Send(reflect.ValueOf(infs))
		return
	}
	if len(infs) == 1 {
		outChan.Send(reflect.ValueOf(infs[0]))
		return
	}
}
