package main

import (
	"flag"
	"fmt"
	"regexp"
	"strings"
	"sync"

	_ "github.com/chrislusf/glow/driver"
	"github.com/chrislusf/glow/flow"
)

var (
	testInt = flag.Int("test", 1, "which test to run")
)

func main() {
	flag.Parse()

	switch *testInt {
	case 1:
		testBasicMapReduce()
	case 2:
		testPartitionAndSort()
	case 3:
		testSelfJoin()
	case 4:
		testJoin()
	case 5:
		testInputOutputChannels()
	case 6:
		testUnrolledStaticLoop()
	}

}

func goStart(wg *sync.WaitGroup, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		fn()
	}()
}

func testBasicMapReduce() {
	flow.New().TextFile(
		"/etc/passwd", 2,
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

func testPartitionAndSort() {
	flow.New().TextFile(
		"/etc/hosts", 7,
	).Partition(
		2,
	).Map(func(line string) string {
		return line
	}).Sort(func(a string, b string) bool {
		if strings.Compare(a, b) < 0 {
			return true
		}
		return false
	}).Map(func(line string) {
		println(line)
	}).Run()

}

func testSelfJoin() {
	words := flow.New().TextFile(
		"/etc/passwd", 3,
	).Filter(func(line string) bool {
		return !strings.HasPrefix(line, "#")
	}).Map(func(line string, ch chan string) {
		for _, token := range strings.Split(line, ":") {
			ch <- token
		}
	}).Map(func(line string) (string, string) {
		return line, line
	})

	words.Join(words).Map(func(key, left, right string) {
		println(key, ":", left, ":", right)
	}).Run()

}

func testJoin() {
	reg, err := regexp.Compile("[^A-Za-z0-9]+")
	if err != nil {
		panic(err)
	}
	tokenizer := func(line string, ch chan string) {
		line = reg.ReplaceAllString(line, "-")
		for _, token := range strings.Split(line, "-") {
			ch <- strings.ToLower(token)
		}
	}
	f1 := flow.New()
	leftWords := f1.TextFile(
		"/etc/passwd", 3,
	).Map(tokenizer).Map(func(t string) (string, int) {
		return t, 1
	}).Sort(nil).LocalReduceByKey(func(x, y int) int {
		return x + y
	})

	rightWords := f1.TextFile(
		"word_count.go", 3,
	).Map(tokenizer).Map(func(t string) (string, int) {
		return t, 1
	}).Sort(nil).LocalReduceByKey(func(x, y int) int {
		return x + y
	})

	leftWords.Join(rightWords).Map(func(key string, left, right int) {
		println(key, ":", left, ":", right)
	}).Run()

}

func testInputOutputChannels() {
	ch := make(chan int)
	f1 := flow.New()
	source := f1.Channel(ch)
	left := source.Map(func(t int) (int, int) {
		return t, t * 2
	})
	right := source.Map(func(t int) (int, int) {
		return t, t * 3
	})

	outChannel := make(chan struct {
		X, Y, Z int
	})

	left.Join(right).AddOutput(outChannel)

	outInt := make(chan int)
	source.AddOutput(outInt)

	flow.Ready()

	var wg sync.WaitGroup
	goStart(&wg, func() {
		f1.Run()
	})

	goStart(&wg, func() {
		for out := range outInt {
			fmt.Printf("source %d \n", out)
		}
	})

	goStart(&wg, func() {
		for out := range outChannel {
			fmt.Printf("%d : %d\n", out.X, out.Y)
		}
	})

	limit := 5
	for i := 0; i < limit; i++ {
		ch <- i
		ch <- i
	}
	close(ch)

	wg.Wait()
}

func testUnrolledStaticLoop() {
	ch := make(chan int)
	f1 := flow.New()
	left := f1.Channel(ch).Partition(2).Map(func(t int) (int, int) {
		return t, t * 2
	})

	for i := 0; i < 7; i++ {
		left = left.Map(func(x, y int) (int, int) {
			return x + 1, y + 1
		})
	}

	outChannel := make(chan struct {
		X, Y int
	})

	left.AddOutput(outChannel)

	flow.Ready()

	var wg sync.WaitGroup
	goStart(&wg, func() {
		f1.Run()
	})

	goStart(&wg, func() {
		for out := range outChannel {
			fmt.Printf("%d : %d\n", out.X, out.Y)
		}
	})

	limit := 5
	for i := 0; i < limit; i++ {
		ch <- i
	}
	close(ch)

	wg.Wait()
}
