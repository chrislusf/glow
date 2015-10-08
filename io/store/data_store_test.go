package store

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
)

func xTestRotate(t *testing.T) {
	s, _ := NewLocalFileDataStore(".", "x")
	s.store.MaxMegaByte = 1

	buf, _ := ioutil.ReadFile("rotating_file_store.go")
	var lines []string
	for i, line := range bytes.Split(buf, []byte("\n")) {
		lines = append(lines, fmt.Sprintf("%d: %s\n", i, line))
	}

	for i := 0; i < 320; i++ {
		for _, line := range lines {
			s.store.Write([]byte(line))
		}
	}

}

func xTestReadOffsets(t *testing.T) {
	s, _ := NewLocalFileDataStore(".", "x")

	for _, ds := range s.store.Segments {
		fmt.Printf("%s: start %d size %d\n", ds.File.Name(), ds.Offset, ds.Size)
	}

	buf := make([]byte, 100)
	s.ReadAt(buf, 2944044)

	fmt.Println(string(buf))
}

func TestReadOffsets(t *testing.T) {
	s, err := NewLocalFileDataStore(".", "x")
	if err != nil {
		println("error: %s", err.Error())
	}

	buf := make([]byte, 4)
	s.ReadAt(buf, 0)

	fmt.Println(string(buf))
}
