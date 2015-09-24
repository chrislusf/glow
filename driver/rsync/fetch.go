package rsync

import (
	"fmt"

	gosync "github.com/Redundancy/go-sync"
	"github.com/Redundancy/go-sync/filechecksum"
	"github.com/Redundancy/go-sync/index"
	"github.com/Redundancy/go-sync/indexbuilder"
)

const (
	BLOCK_SIZE = 64 * 1024
)

// TODO: this does not work yet. leave it here for later
// temporarily use FetchUrl() for now
// http://localhost:port/content
func Fetch(fileUrl string, destFile string) {

	referenceFileIndex, checksumLookup, fileSize, _ := fetchIndex("somewhere")

	blockCount := fileSize / BLOCK_SIZE
	if fileSize%BLOCK_SIZE != 0 {
		blockCount++
	}

	fs := &gosync.BasicSummary{
		ChecksumIndex:  referenceFileIndex,
		ChecksumLookup: checksumLookup,
		BlockCount:     uint(blockCount),
		BlockSize:      uint(BLOCK_SIZE),
		FileSize:       fileSize,
	}

	rsyncObject, err := gosync.MakeRSync(
		destFile,
		fileUrl,
		destFile,
		fs,
	)

	err = rsyncObject.Patch()

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	err = rsyncObject.Close()

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

}

// this needs change
const (
	REFERENCE = ""
)

func fetchIndex(indexFileUrl string) (referenceFileIndex *index.ChecksumIndex, checksumLookup filechecksum.ChecksumLookup, fileSize int64, err error) {
	generator := filechecksum.NewFileChecksumGenerator(BLOCK_SIZE)

	_, referenceFileIndex, checksumLookup, err = indexbuilder.BuildIndexFromString(generator, REFERENCE)

	if err != nil {
		return
	}

	fileSize = int64(len([]byte(REFERENCE)))

	return
}
