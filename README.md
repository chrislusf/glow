# glow

Examples are in this repo https://github.com/chrislusf/glow_examples

[![GoDoc](https://godoc.org/github.com/chrislusf/glow?status.svg)](https://godoc.org/github.com/chrislusf/glow)

# Purpose

Glow is providing a library to easily compute in parallel threads or distributed to clusters of machines.

# Installation
```
go get github.com/chrislusf/glow
go get github.com/chrislusf/glow/flow
```

# One minute tutorial

## Simple Start

Here is a simple full example:

```
package main

import (
	"flag"
	"strings"

	"github.com/chrislusf/glow/flow"
)

func main() {
	flag.Parse()

	flow.New().TextFile(
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
	}).Run()
}

```

Try it. 
```
./word_count
```

It will run the input text file, '/etc/passwd', in 3 go routines, filter/map/map, and then reduced to one number in one goroutine (not exactly one goroutine, but let's skip the details for now.) and print it out. 

This is useful already, saving lots of idiomatic but repetitive code on channels, sync wait, etc.

However, there is one more thing!

## Scale it out
We need to setup the cluster first. We do not need experts on Zookeeper/HDFS/Mesos/YARN etc. Just need to build or download one binary file.

### Setup the cluster
```
  // fetch and install via go, or just download it from somewhere
  go get github.com/chrislusf/glow
  // start a master on one computer
  glow master
  // run one or more agents on computers
  glow agent --dir . --max.executors=16 --memory=2048 --master="localhost:8930" --port 8931
```
### Start the driver program
To leap from one computer to clusters of computers, add this line to the import list:

```
	_ "github.com/chrislusf/glow/driver"
```
This will "steroidize" the code to run in cluster mode! 

```
./word_count -glow -glow.leader="localhost:8930"
```

![Glow Hello World Execution Plan](https://raw.githubusercontent.com/chrislusf/glow/master/etc/helloworld.png)


# Read More

1. [Glow Introduction Slides] (https://raw.githubusercontent.com/chrislusf/glow/master/etc/GlowIntroduction.pdf)
2. Wiki page: https://github.com/chrislusf/glow/wiki
3. Mailing list: https://groups.google.com/forum/#!forum/glow-user-discussion
4. Examples: https://github.com/chrislusf/glow_examples/tree/master/word_count

# Contribution
Fork it, code it, and send pull requests. Better first discuss about the feature you want on the mailing list.
https://groups.google.com/forum/#!forum/glow-user-discussion

# License
http://www.apache.org/licenses/LICENSE-2.0
