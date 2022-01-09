package squirrel

import "zombiezen.com/go/sqlite"

// Wraps a specific sqlite.Blob instance, when we don't want to dive into the cache to refetch blobs.
type PinnedBlob struct {
	sb *sqlite.Blob
	c  *Cache
}

// This is very cheap for this type.
func (pb PinnedBlob) Length() int64 {
	return pb.sb.Size()
}

// Requires only that we lock the sqlite conn.
func (pb PinnedBlob) ReadAt(b []byte, off int64) (int, error) {
	pb.c.l.Lock()
	defer pb.c.l.Unlock()
	return blobReadAt(pb.sb, b, off)
}