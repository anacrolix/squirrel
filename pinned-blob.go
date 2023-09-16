package squirrel

import (
	"errors"
	"fmt"
	"time"

	g "github.com/anacrolix/generics"
	"github.com/go-llsqlite/adapter"
)

// Wraps a specific sqlite.Blob instance, when we don't want to dive into the cache to refetch
// blobs. Until Closed, PinnedBlob holds a transaction open on the Cache.
type PinnedBlob struct {
	key   string
	rowid int64
	blob  *sqlite.Blob
	c     *Cache
	write bool
	tx    *Tx
}

func (pb *PinnedBlob) Reopen(name string) error {
	if pb.tx == nil {
		pb.c.l.Lock()
		defer pb.c.l.Unlock()
	}
	rowid, _, ok, err := rowidForBlob(pb.c.conn, name)
	// If we fail between here and the reopen, the blob handle remains on the existing row.
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("rowid for name not found")
	}
	// If this fails, the blob handle is aborted.
	return pb.blob.Reopen(rowid)
}

// This is very cheap for this type.
func (pb *PinnedBlob) Length() int64 {
	return pb.blob.Size()
}

// Requires only that we lock the sqlite conn.
func (pb *PinnedBlob) ReadAt(b []byte, off int64) (n int, err error) {
	return pb.doIoAt(blobReadAt, b, off)
}

func (pb *PinnedBlob) WriteAt(b []byte, off int64) (int, error) {
	return pb.doIoAt(blobWriteAt, b, off)
}

func (pb *PinnedBlob) doIoAt(
	// Naming inspired by sqlite3 internals
	xCall func(*sqlite.Blob, []byte, int64) (int, error),
	b []byte,
	off int64,
) (n int, err error) {
	if pb.tx == nil {
		pb.c.l.Lock()
		defer pb.c.l.Unlock()
	}
	for {
		n, err = xCall(pb.blob, b, off)
		if n != 0 {
			_, accessErr := pb.c.accessBlob(pb.key)
			if !pb.write && sqlite.IsResultCode(accessErr, sqlite.ResultCodeBusy) {
				accessErr = nil
			}
			if accessErr != nil {
				err = errors.Join(err, accessErr)
			}
			return
		}
		if !isReopenBlobError(err) {
			return
		}
		pb.blob.Close()
		pb.blob, pb.rowid, err = pb.c.getBlob(pb.key, false, -1, false, g.Some(pb.rowid), pb.write)
		if err != nil {
			err = fmt.Errorf("blob expired, error reopening: %w", err)
			return
		}
		b = b[n:]
		off += int64(n)
	}
}

func isReopenBlobError(err error) bool {
	return errors.Is(err, sqlite.ErrBlobClosed) || sqlite.IsResultCode(err, sqlite.ResultCodeAbort)
}

func (pb *PinnedBlob) Close() error {
	// pb.blob can be nil if reopening failed.
	if pb.blob != nil {
		return pb.blob.Close()
	}
	return nil
}

func (pb *PinnedBlob) LastUsed() (lastUsed time.Time, err error) {
	if pb.tx == nil {
		pb.c.l.Lock()
		defer pb.c.l.Unlock()
	}
	return pb.c.lastUsed(pb.key)
}
