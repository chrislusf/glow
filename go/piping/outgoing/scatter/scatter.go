// scatter takes one input file and scatters the content to multiple output files
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/vova616/xxhash"
)

var (
	hashField = flag.Int("hash", 0, "The field to hash, starting from 1.")
	lineLimit = flag.Int("line", 0, "The number of lines to split on.")
	separator = flag.String("d", " ", "Field separator, default to empty space.")
)

func main() {

	flag.Parse()

	if flag.NArg() <= 1 {
		fmt.Fprintf(os.Stderr, "Need one input and some output files!")
		flag.Usage()
		return
	}

	// These are output files or pipes
	outputFiles := flag.Args()[1:]
	fmt.Println("output files:", len(outputFiles))

	// channels of []byte to pass to writing process
	streams := make([]chan []byte, len(outputFiles))
	for i := 0; i < len(streams); i++ {
		streams[i] = make(chan []byte)
	}

	// start writing go routines
	wg := goStart(len(outputFiles), func(i int) {
		f, err := os.OpenFile(outputFiles[i], os.O_WRONLY|os.O_APPEND, 0666)
		check(err)
		defer f.Close()
		fmt.Println("output files:", outputFiles[i])

		for buf := range streams[i] {
			f.Write(buf)
		}
	})

	// read each line of the input, distribute to outputs
	sep := (*separator)[0]
	readFileAndDistribute(flag.Arg(0), streams,
		func(line []byte, streamsCount int) int {
			i := 0
			for ; i < len(line); i++ {
				if line[i] == sep {
					break
				}
			}
			return int(xxhash.Checksum32(line[:i])) % streamsCount
		})

	// close all outputs
	for i := 0; i < len(outputFiles); i++ {
		close(streams[i])
	}

	// wait for output writing to finish
	wg.Wait()

}

// goStart starts go routines and return a wait group
func goStart(count int, goFun func(int)) sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			goFun(i)

		}(i)
	}
	return wg
}

func readFileAndDistribute(filename string, outputs []chan []byte,
	chooseDestinationFn func([]byte, int) int) {
	fmt.Println("Reading file", filename)
	file, err := os.Open(filename)
	check(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	dest := 0
	outputCount := len(outputs)
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		dest = chooseDestinationFn(scanner.Bytes(), outputCount)
		fmt.Printf("%d: %s\n", dest, string(scanner.Bytes()))
		outputs[dest] <- scanner.Bytes()
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
