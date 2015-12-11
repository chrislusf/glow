package hdfs

import (
	"bufio"
	"fmt"
	"log"
	"strings"

	"github.com/chrislusf/glow/flow"
	"github.com/colinmarc/hdfs"
)

// Source lists files under a hdfs folder, and process all files
// This is provided more as an example. You can copy the code and customize
// any way you want.
func Source(f *flow.FlowContext, hdfsLocation string, shard int) *flow.Dataset {
	locations, err := List(hdfsLocation)
	if err != nil {
		log.Fatalf("Can not list files under %s:%v", hdfsLocation, err)
	}

	return f.Slice(locations).Partition(shard).Map(TextFile)
}

// List generates a full list of file locations under the given
// location, which should have a prefix of hdfs://
func List(hdfsLocation string) (locations []string, err error) {

	namenode, path, err := splitLocationToParts(hdfsLocation)
	if err != nil {
		return
	}

	client, err := hdfs.New(namenode)
	if err != nil {
		return nil, fmt.Errorf("failed to create client to %s:%v\n", namenode, err)
	}

	fileInfos, err := client.ReadDir("/" + path)
	if err != nil {
		return nil, fmt.Errorf("failed to list files under /%s:%v\n", path, err)
	}

	for _, fi := range fileInfos {
		locations = append(locations, hdfsLocation+"/"+fi.Name())
	}

	return

}

func splitLocationToParts(location string) (namenode, path string, err error) {
	hdfsPrefix := "hdfs://"
	if !strings.HasPrefix(location, hdfsPrefix) {
		return "", "", fmt.Errorf("parameter %s should start with hdfs://", location)
	}

	parts := strings.SplitN(location[len(hdfsPrefix):], "/", 2)
	return parts[0], "/" + parts[1], nil
}

func TextFile(location string, lines chan string) {
	namenode, path, err := splitLocationToParts(location)
	if err != nil {
		return
	}

	client, err := hdfs.New(namenode)
	if err != nil {
		log.Fatalf("failed to create client to %s:%v\n", namenode, err)
	}

	file, err := client.Open(path)

	if err != nil {
		log.Fatalf("Can not open file %s: %v", location, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines <- scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Scan file %s: %v", location, err)
	}
}
