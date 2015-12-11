package main

import (
	"flag"
	"strings"

	_ "github.com/chrislusf/glow/driver"
	"github.com/chrislusf/glow/flow"
	"github.com/chrislusf/glow/source/hdfs"
)

var ()

func main() {
	flag.Parse()

	f := flow.New()
	hdfs.Source(
		f,
		"hdfs://localhost:9000/etc",
		8,
	).Filter(func(line string) bool {
		// println("filter:", line)
		return !strings.HasPrefix(line, "#")
	}).Map(func(line string, ch chan string) {
		for _, token := range strings.Split(line, ":") {
			ch <- token
		}
	}).Map(func(key string) int {
		// println("map:", key)
		return 1
	}).Reduce(func(x int, y int) int {
		// println("reduce:", x+y)
		return x + y
	}).Map(func(x int) {
		println("count:", x)
	}).Run()
}
