package squirrel

import (
	"io"

	"zombiezen.com/go/sqlite"
)

func blobReadAt(blob *sqlite.Blob, b []byte, off int64) (n int, err error) {
	_, err = blob.Seek(off, io.SeekStart)
	if err != nil {
		return
	}
	n, err = blob.Read(b)
	return
}

func blobWriteAt(blob *sqlite.Blob, b []byte, off int64) (n int, err error) {
	_, err = blob.Seek(off, io.SeekStart)
	if err != nil {
		return
	}
	n, err = blob.Write(b)
	return
}
