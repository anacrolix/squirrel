package squirrel

import (
	"github.com/go-llsqlite/adapter"
)

// Blob API differs between crawshaw and zombiezen.

func blobReadAt(blob *sqlite.Blob, b []byte, off int64) (n int, err error) {
	return blob.ReadAt(b, off)
}
func blobWriteAt(blob *sqlite.Blob, b []byte, off int64) (n int, err error) {
	return blob.WriteAt(b, off)
}
