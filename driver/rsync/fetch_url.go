package rsync

import (
	"io/ioutil"
	"log"

	"github.com/chrislusf/glow/util"
)

func FetchUrl(fileUrl string, destFile string) {
	_, buf, err := util.DownloadUrl(fileUrl)
	if err != nil {
		log.Printf("Failed to read from %s: %v", fileUrl, err)
	}
	err = ioutil.WriteFile(destFile, buf, 0755)
	if err != nil {
		log.Printf("Failed to write %s: %v", destFile, err)
	}
}
