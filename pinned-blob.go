package squirrel

import (
	"errors"
	"io"
	"time"

	"github.com/go-llsqlite/adapter"
)

// Wraps a specific sqlite.Blob instance, when we don't want to dive into the cache to refetch
// blobs. Until Closed, PinnedBlob holds a transaction open on the Cache.
type PinnedBlob struct {
	key     string
	write   bool
	tx      *Tx
	valueId rowid
}

// This is very cheap for this type.
func (pb *PinnedBlob) Length() int64 {
	l, err := pb.LengthErr()
	if err != nil {
		return -1
	}
	return l
}

func (pb *PinnedBlob) closedErr() error {
	if pb.tx == nil {
		return ErrClosed
	}
	return nil
}

// This is very cheap for this type.
func (pb *PinnedBlob) LengthErr() (_ int64, err error) {
	err = pb.closedErr()
	if err != nil {
		return
	}
	return pb.tx.conn.getValueLength(pb.key)
}

// Requires only that we lock the sqlite conn.
func (pb *PinnedBlob) ReadAt(b []byte, valueOff int64) (n int, err error) {
	return pb.doIoAt(b, valueOff, (*sqlite.Blob).ReadAt, false)
}

// Requires only that we lock the sqlite conn.
func (pb *PinnedBlob) doIoAt(
	b []byte,
	valueOff int64,
	blobCall func(*sqlite.Blob, []byte, int64) (int, error),
	write bool,
) (n int, err error) {
	err = pb.closedErr()
	if err != nil {
		return
	}
	conn := pb.tx.conn
	l, err := conn.getValueLength(pb.key)
	if err != nil {
		return
	}
	if valueOff >= l {
		err = io.EOF
		return
	}
	err = conn.iterBlobs(
		pb.valueId,
		func(blobOff int64, blob *sqlite.Blob) (more bool, err error) {
			readOff := valueOff - blobOff
			if readOff < 0 {
				return false, nil
			}
			if readOff >= blob.Size() {
				return true, nil
			}
			b1 := b
			if int64(len(b1)) > blob.Size()-readOff {
				b1 = b[:blob.Size()-readOff]
			}
			n1, err := blobCall(blob, b1, readOff)
			n += n1
			b = b[n1:]
			valueOff += int64(n1)
			if n1 == len(b1) && err == io.EOF {
				err = nil
			}
			if err != nil {
				return
			}
			more = len(b) != 0
			return
		},
		write,
		valueOff,
	)
	if n != 0 {
		err = errors.Join(err, conn.accessedKey(pb.valueId, !write))
	}
	return
}

func (pb *PinnedBlob) WriteAt(b []byte, off int64) (n int, err error) {
	return pb.doIoAt(b, off, (*sqlite.Blob).WriteAt, true)
}

func (pb *PinnedBlob) Close() error {
	pb.tx = nil
	return nil
}

func (pb *PinnedBlob) LastUsed() (lastUsed time.Time, err error) {
	err = pb.closedErr()
	if err != nil {
		return
	}
	return pb.tx.conn.lastUsed(pb.valueId)
}
