package squirrel

import (
	"errors"
	"fmt"
	"time"

	g "github.com/anacrolix/generics"
	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

// Blobs are references to a name in a Cache that are looked up when its methods are used. They
// should not be when inside a Tx.
type Blob struct {
	name   string
	length g.Option[int64]
	cache  *Cache
}

func (p Blob) getBlob(write bool) (*sqlite.Blob, rowid, error) {
	return p.cache.getBlob(p.name, write, p.length.UnwrapOr(-1), false, g.None[int64](), write)
}

func (p Blob) doWithBlob(
	withBlob func(*sqlite.Blob) error,
	create bool,
) (err error) {
	blob, _, err := p.getBlob(create)
	if err != nil {
		err = fmt.Errorf("getting sqlite blob: %w", err)
		return
	}
	err = withBlob(blob)
	return errors.Join(err, blob.Close())
}

func (p Blob) ReadAt(b []byte, off int64) (n int, err error) {
	err = p.cache.Tx(func(tx *Tx) error {
		return p.doWithBlob(func(blob *sqlite.Blob) (err error) {
			n, err = blobReadAt(blob, b, off)
			return
		}, false)
	})
	return
}

func (p Blob) WriteAt(b []byte, off int64) (n int, err error) {
	err = p.cache.Tx(func(tx *Tx) error {
		return p.doWithBlob(func(blob *sqlite.Blob) (err error) {
			n, err = blobWriteAt(blob, b, off)
			return
		}, true)
	})
	return
}

func (p Blob) SetTag(name string, value interface{}) (err error) {
	return p.cache.SetTag(p.name, name, value)
}

type SqliteStmt = *sqlite.Stmt

func (p Blob) GetTag(name string, result func(stmt SqliteStmt)) error {
	return p.cache.Tx(func(tx *Tx) error {
		return sqlitex.Exec(p.cache.conn, "select value from tag where blob_name=? and tag_name=?", func(stmt *sqlite.Stmt) error {
			result(stmt)
			return nil
		}, p.name, name)
	})
}

func (b Blob) Delete() error {
	return b.cache.Tx(func(tx *Tx) error {
		return sqlitex.Execute(b.cache.conn, "delete from blob where name=?", &sqlitex.ExecOptions{
			Args: []interface{}{b.name},
		})
	})
}

func (b Blob) Size() (l int64, err error) {
	err = b.cache.Tx(func(tx *Tx) error {
		return b.doWithBlob(func(blob *sqlite.Blob) error {
			l = blob.Size()
			return nil
		}, false)
	})
	return
}

func (b Blob) LastUsed() (lastUsed time.Time, err error) {
	b.cache.l.RLock()
	defer b.cache.l.RUnlock()
	err = b.cache.getCacheErr()
	if err != nil {
		return
	}
	return b.cache.lastUsed(b.name)
}
