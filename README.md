# glow

Examples are in this repo https://github.com/chrislusf/glow

# Purpose

Glow is providing a library to easily compute in parallel threads or distributed to clusters of machines.

# 1 minute tutorial

## Simple Start

Here is an simple full example:

```
package main

import (
	"flag"
	"log"
	"regexp"
	"strings"

	"github.com/chrislusf/glow/flow"
)

func main() {
	flag.Parse()

	flow.NewContext().TextFile(
		"/etc/passwd", 3,
	).Filter(func(line string) bool {
		return !strings.HasPrefix(line, "#")
	}).Map(func(line string, ch chan string) {
		for _, token := range strings.Split(line, ":") {
			ch <- token
		}
	}).Map(func(key string) int {
		return 1
	}).Reduce(func(x int, y int) int {
		return x + y
	}).Map(func(x int) {
		println("count:", x)
	})
}

```

Try it. 
```
./word_count
```

It will run the input text file, '/etc/passwd', in 3 go routines, filter/map/map, and then reduced to one number in one goroutine (not exactly correct, but let's skip the details for now.) and print it out. 

This is useful already, saving lots of idiomatic but repetitive code on channels, sync wait. However, there is one more thing.

## Scale it out
We need to setup the cluster first.

### Setup the cluster
```
  // fetch and install
  go get github.com/chrislusf/glow
  // start a leader on one computer
  glow leader
  // run one or more agents on computers
  glow agent --dir . --max.executors=16 --memory=2048 --leader="localhost:8930" --port 8931
```
### start the driver program
To leap from one computer to clusters of computers, add this line to the import list:

```
	_ "github.com/chrislusf/glow/driver"
```
This will "steroidize" the code to run in cluster mode! 

```
./word_count -driver -driver.leader="localhost:8930"
```
