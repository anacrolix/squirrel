package squirrel

import "github.com/go-llsqlite/adapter"

// Wraps a specific sqlite.Blob instance, when we don't want to dive into the cache to refetch blobs.
type PinnedBlob struct {
	*sqlite.Blob
	c *Cache
}

// This is very cheap for this type.
func (pb PinnedBlob) Length() int64 {
	return pb.Size()
}

// Requires only that we lock the sqlite conn.
func (pb PinnedBlob) ReadAt(b []byte, off int64) (int, error) {
	pb.c.l.Lock()
	defer pb.c.l.Unlock()
	return blobReadAt(pb.Blob, b, off)
}
