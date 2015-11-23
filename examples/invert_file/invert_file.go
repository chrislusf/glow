package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"strings"

	_ "github.com/chrislusf/glow/driver"
	"github.com/chrislusf/glow/flow"
)

type WordSentence struct {
	Word       string
	LineNumber int
}

var (
	fileName = flag.String("file", "/etc/passwd", "name of a text file")
	f1       = flow.New()
	f2       = flow.New()
)

func init() {
	f1.Source(func(out chan WordSentence) {
		bytes, err := ioutil.ReadFile(*fileName)
		if err != nil {
			println("Failed to read", *fileName)
			return
		}
		lines := strings.Split(string(bytes), "\n")
		for lineNumber, line := range lines {
			for _, word := range strings.Split(line, " ") {
				if word != "" {
					out <- WordSentence{word, lineNumber}
				}
			}
		}
	}, 3).Map(func(ws WordSentence) (string, int) {
		return ws.Word, ws.LineNumber
	}).GroupByKey().Map(func(word string, lineNumbers []int) {
		fmt.Printf("%s : %v\n", word, lineNumbers)
	})

	f2.TextFile(
		"/etc/passwd", 2,
	).Map(func(line string, ch chan string) {
		for _, token := range strings.Split(line, " ") {
			ch <- token
		}
	}).Map(func(key string) (string, int) {
		return key, 1
	}).ReduceByKey(func(x int, y int) int {
		// println("reduce:", x+y)
		return x + y
	}).Map(func(key string, x int) {
		println(key, ":", x)
	})

}

func main() {
	flag.Parse()
	flow.Ready()
	f1.Run()

	f2.Run()

}
