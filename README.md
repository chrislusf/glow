# glow
[![Build Status](https://travis-ci.org/chrislusf/glow.svg?branch=master)](https://travis-ci.org/chrislusf/glow)
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
  > ./word_count
```

It will run the input text file, '/etc/passwd', in 3 go routines, filter/map/map, and then reduced to one number in one goroutine (not exactly one goroutine, but let's skip the details for now.) and print it out. 

This is useful already, saving lots of idiomatic but repetitive code on channels, sync wait, etc, to fully utilize more CPU cores.

However, there is one more thing! It can run across a Glow cluster, which can be run multiple servers/racks/data centers!

## Scale it out
To setup the Glow cluster, we do not need experts on Zookeeper/HDFS/Mesos/YARN etc. Just build or download one binary file.

### Setup the cluster
```
  // fetch and install via go, or just download it from somewhere
  > go get github.com/chrislusf/glow
  // start a master on one computer
  > glow master
  // run one or more agents on computers
  > glow agent --dir . --max.executors=16 --memory=2048 --master="localhost:8930" --port 8931
```
Glow Master and Glow Agent run very efficiently. They take about 6.5MB and 5.5MB memory respectively in my environments. I would recommend set up agents on any server you can find. You can tap into the computing power whenever you need to.

### Start the driver program
To leap from one computer to clusters of computers, add this line to the import list:

```
	_ "github.com/chrislusf/glow/driver"
```
This will "steroidize" the code to run in cluster mode! 

```
> ./word_count -glow -glow.leader="localhost:8930"
```
The word_count program will become a driver program, dividing the execution into a directed acyclic graph(DAG), and send tasks to agents.

### Visualize the flow

To understand how each executor works, you can visualize the flow by generating a dot file of the flow, and render it to png file via "dot" command provided from graphviz.
```
> ./word_count -glow -glow.flow.plot > x.dot
> dot -Tpng -otestSelfJoin.png x.dot
```

![Glow Hello World Execution Plan](https://raw.githubusercontent.com/chrislusf/glow/master/etc/helloworld.png)

## Docker container

### OSX
Cross compile artefact for docker
```
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build .
```
Build container
```
docker build -t glow .
```
See `examples/` directory for docker-compose setups.

# Read More

1. Wiki page: https://github.com/chrislusf/glow/wiki
2. Mailing list: https://groups.google.com/forum/#!forum/glow-user-discussion
3. Examples: https://github.com/chrislusf/glow/tree/master/examples/

# Contribution
Start using it! And report or fix any issue you have seen, add any feature you want.

Fork it, code it, and send pull requests. Better first discuss about the feature you want on the mailing list.
https://groups.google.com/forum/#!forum/glow-user-discussion

# License
http://www.apache.org/licenses/LICENSE-2.0
