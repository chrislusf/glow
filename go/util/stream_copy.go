package util

import (
	"io"
	"time"
)

func StreamCopy(src io.Reader, dst io.Writer, buf []byte, name string) (written int64, err error) {
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				return
			}
			if nr != nw {
				err = io.ErrShortWrite
				return
			}
		}
		print(name, ">", time.Now().Unix(), " len ", nr, "\n")
		if er != nil {
			err = er
			return
		}
	}
}

func IoCopy(dst io.Writer, src io.Reader, name string) (written int64, err error) {
	buf := make([]byte, 64*1024)
	StreamCopy(src, dst, buf, name)
	return 0, nil
}
