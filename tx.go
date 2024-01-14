package squirrel

import (
	"errors"
	g "github.com/anacrolix/generics"
	sqlite "github.com/go-llsqlite/adapter"
	"io"
	"time"
)

type Tx struct {
	conn         conn
	accessedKeys map[rowid]struct{}
	write        bool
}

type CreateOpts struct {
	Length int64
}

func (tx *Tx) Create(name string, opts CreateOpts) (pb *PinnedBlob, err error) {
	keyId, err := tx.conn.createKey(name, opts)
	if err != nil {
		return
	}
	pb = &PinnedBlob{
		key:     name,
		write:   true,
		tx:      tx,
		valueId: keyId,
	}
	return
}

func (tx *Tx) Open(name string) (pb *PinnedBlob, err error) {
	var keyId setOnce[rowid]
	err = tx.conn.sqliteQuery(
		`select key_id from keys where key=?`,
		func(stmt *sqlite.Stmt) error {
			keyId.Set(stmt.ColumnInt64(0))
			return nil
		},
		name,
	)
	pb = &PinnedBlob{
		key:     name,
		write:   true,
		tx:      tx,
		valueId: keyId.Value(),
	}
	return
}

func (tx *Tx) Put(name string, b []byte) (err error) {
	err = tx.Delete(name)
	if err != nil && err != ErrNotFound {
		return
	}
	pb, err := tx.Create(name, CreateOpts{int64(len(b))})
	if err != nil {
		return
	}
	_, err = pb.WriteAt(b, 0)
	err = errors.Join(err, pb.Close())
	return
}

func (tx *Tx) ReadAll(key string, b []byte) (ret []byte, err error) {
	conn := tx.conn
	keyCols, err := conn.openKey(key)
	if err != nil {
		return
	}
	if int64(len(b)) < keyCols.length {
		b = make([]byte, keyCols.length)
	} else {
		b = b[:keyCols.length]
	}
	n, err := tx.readFull(keyCols.id, b)
	ret = b[:n]
	return
}

func (tx *Tx) ReadFull(key string, b []byte) (n int, err error) {
	valueId, err := tx.conn.getValueIdForKey(key)
	if err != nil {
		return
	}
	return tx.readFull(valueId, b)
}

func (tx *Tx) readFull(valueId rowid, b []byte) (n int, err error) {
	var nextOff int64
	b0 := b
	err = tx.conn.iterBlobs(
		valueId,
		func(offset int64, blob *sqlite.Blob) (more bool, err error) {
			if offset > nextOff {
				err = io.EOF
				return
			}
			n1, err := blob.ReadAt(b, nextOff-offset)
			n += n1
			b = b[n1:]
			nextOff += int64(n1)
			more = len(b) != 0
			return
		},
		false,
		0,
	)
	if err == io.EOF {
		if n == len(b0) {
			err = nil
		} else {
			err = io.ErrUnexpectedEOF
		}
	}
	return
}

func (tx *Tx) SetTag(key, name string, value any) (err error) {
	cols, err := tx.conn.openKey(key)
	if err != nil {
		return
	}
	return tx.conn.sqliteQuery(
		"insert or replace into tags (key_id, tag_name, value) values (?, ?, ?)",
		nil,
		cols.id,
		name,
		value,
	)
}

func (tx *Tx) Delete(name string) (err error) {
	return tx.conn.deleteKey(name)
}

// Returns a PinnedBlob. The item must already exist. You must call PinnedBlob.Close when done
// with it.
func (tx *Tx) OpenPinned(name string) (ret *PinnedBlob, err error) {
	return tx.openPinned(name, true)
}

// Returns a PinnedBlob. The item must already exist. You must call PinnedBlob.Close when done
// with it.
func (tx *Tx) OpenPinnedReadOnly(name string) (ret *PinnedBlob, err error) {
	return tx.openPinned(name, false)
}

func (tx *Tx) lastUsed(keyId rowid) (t time.Time, err error) {
	if g.MapContains(tx.accessedKeys, keyId) {
		return time.Now(), nil
	}
	return tx.conn.lastUsed(keyId)
}
