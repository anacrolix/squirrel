package squirrel

import (
	"errors"
	"fmt"
	g "github.com/anacrolix/generics"
	"io/fs"
	"strings"

	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

type Blob struct {
	name   string
	length int64
	cache  *Cache
}

func (p Blob) getBlob(create, clobberLength bool) (*sqlite.Blob, rowid, error) {
	return p.cache.getBlob(p.name, create, p.length, clobberLength, g.None[int64]())
}

// In the crawshaw implementation, this was an isolated error message value. In zombiezen, it's
// produced by errors.New and wrapped. There's no way I know of to isolate it.
func isErrInvalidBlob(err error) bool {
	return strings.HasSuffix(err.Error(), "invalid blob")
}

func (p Blob) doWithBlob(
	withBlob func(*sqlite.Blob) error,
	create bool,
	clobberLength bool,
) (err error) {
	defer p.forgetBlob()
	blob, _, err := p.getBlob(create, clobberLength)
	if err != nil {
		err = fmt.Errorf("getting sqlite blob: %w", err)
		return
	}
	err = withBlob(blob)
	return errors.Join(err, blob.Close())
}

func (p Blob) ReadAt(b []byte, off int64) (n int, err error) {
	p.cache.l.Lock()
	defer p.cache.l.Unlock()
	err = p.cache.getCacheErr()
	if err != nil {
		return
	}
	err = p.doWithBlob(func(blob *sqlite.Blob) (err error) {
		n, err = blobReadAt(blob, b, off)
		return
	}, false, false)
	return
}

func (p Blob) WriteAt(b []byte, off int64) (n int, err error) {
	p.cache.l.Lock()
	defer p.cache.l.Unlock()
	err = p.cache.getCacheErr()
	if err != nil {
		return
	}
	err = p.doWithBlob(func(blob *sqlite.Blob) (err error) {
		n, err = blobWriteAt(blob, b, off)
		return
	}, true, false)
	return
}

func (p Blob) SetTag(name string, value interface{}) (err error) {
	return p.cache.SetTag(p.name, name, value)
}

func (p Blob) forgetBlob() {
	blob, ok := p.cache.blobs[p.name]
	if !ok {
		return
	}
	blob.Close()
	delete(p.cache.blobs, p.name)
}

type SqliteStmt = *sqlite.Stmt

func (p Blob) GetTag(name string, result func(stmt SqliteStmt)) (err error) {
	p.cache.l.Lock()
	defer p.cache.l.Unlock()
	err = p.cache.getCacheErr()
	if err != nil {
		return
	}
	return sqlitex.Exec(p.cache.conn, "select value from tag where blob_name=? and tag_name=?", func(stmt *sqlite.Stmt) error {
		result(stmt)
		return nil
	}, p.name, name)
}

func (c *Cache) openBlob(rowid int64) (*sqlite.Blob, error) {
	return c.conn.OpenBlob("main", "blob_data", "data", rowid, true)
}

func (c *Cache) getBlob(
	name string,
	create bool,
	length int64,
	clobberLength bool,
	rowidHint g.Option[int64],
) (blob *sqlite.Blob, rowid rowid, err error) {
	blob, ok := c.blobs[name]
	if ok {
		if !clobberLength || length == blob.Size() {
			return
		}
		blob.Close()
	}
	if rowidHint.Ok {
		blob, err = c.openBlob(rowidHint.Value)
		if err == nil {
			rowid = rowidHint.Value
		} else if sqlite.IsResultCode(err, sqlite.ResultCodeGenericError) {
			err = nil
			blob = nil
		} else {
			panic(err)
		}
	}
	if blob == nil {
		var existingLength int64
		var ok bool
		rowid, existingLength, ok, err = rowidForBlob(c.conn, name)
		// log.Printf("got rowid %v, existing length %v, ok %v", rowid, existingLength, ok)
		if err != nil {
			err = fmt.Errorf("getting rowid for blob: %w", err)
			return
		}
		if !ok && !create {
			err = fs.ErrNotExist
			return
		}
		if !ok || (clobberLength && existingLength != length) {
			rowid, err = createBlob(c.conn, name, length, ok && clobberLength && length != existingLength)
			if err != nil {
				err = fmt.Errorf("creating blob: %w", err)
				return
			}
		}
		blob, err = c.openBlob(rowid)
		if err != nil {
			panic(err)
		}
	}
	return blob, rowid, nil
}

func (b Blob) Delete() {
	b.cache.l.Lock()
	defer b.cache.l.Unlock()
	sqlitex.Execute(b.cache.conn, "delete from blob where name=?", &sqlitex.ExecOptions{
		Args: []interface{}{b.name},
	})
}
