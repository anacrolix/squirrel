package squirrel

import (
	"fmt"
	"io"

	"zombiezen.com/go/sqlite"
)

// Blob API differs between crawshaw and zombiezen.

func blobReadAt(blob *sqlite.Blob, b []byte, off int64) (n int, err error) {
	origOff, err := blob.Seek(0, io.SeekCurrent)
	if err != nil {
		err = fmt.Errorf("getting current offset: %w", err)
		return
	}
	_, err = blob.Seek(off, io.SeekStart)
	if err != nil {
		return
	}
	n, err = blob.Read(b)
	if err != nil {
		return
	}
	finalOff, err := blob.Seek(origOff, io.SeekStart)
	if err != nil {
		err = fmt.Errorf("seeking to original offset: %w", err)
		return
	}
	if finalOff != origOff {
		panic("original offset not restored and no error given")
	}
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
