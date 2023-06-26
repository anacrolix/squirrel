package squirrel

import (
	"fmt"
	"io/fs"
	"runtime"
	"strings"

	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

type Blob struct {
	name   string
	length int64
	cache  *Cache
}

func (p Blob) getBlob(create, clobberLength bool) (*sqlite.Blob, error) {
	return p.cache.getBlob(p.name, create, p.length, clobberLength)
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
	p.cache.l.Lock()
	defer p.cache.l.Unlock()
	err = p.cache.getCacheErr()
	if err != nil {
		return
	}
	if p.cache.opts.NoCacheBlobs {
		defer p.forgetBlob()
	}
	// log.Printf("getting blob")
	blob, err := p.getBlob(create, clobberLength)
	if err != nil {
		err = fmt.Errorf("getting sqlite blob: %w", err)
		return
	}
	err = withBlob(blob)
	if err == nil || p.cache.opts.NoCacheBlobs {
		return
	}
	src := sqlite.ErrCode(err)
	// "ABORT" occurs if the row the blob is on is modified elsewhere. "ERROR: invalid blob" occurs
	// if the blob has been closed. We don't forget blobs that are closed by our GC finalizers,
	// because they may be attached to names that have since moved on to another blob.
	if src != sqlite.ResultCodeAbort && !(p.cache.opts.GcBlobs && src == sqlite.ResultCodeGenericError && isErrInvalidBlob(err)) {
		return
	}
	p.forgetBlob()
	// Try again, this time we're guaranteed to get a fresh blob, and so errors are no excuse. It
	// might be possible to skip to this version if we don't cache blobs.
	blob, err = p.getBlob(create, clobberLength)
	if err != nil {
		err = fmt.Errorf("getting blob: %w", err)
		return
	}
	return withBlob(blob)
}

func (p Blob) ReadAt(b []byte, off int64) (n int, err error) {
	err = p.doWithBlob(func(blob *sqlite.Blob) (err error) {
		n, err = blobReadAt(blob, b, off)
		return
	}, false, false)
	return
}

func (p Blob) WriteAt(b []byte, off int64) (n int, err error) {
	err = p.doWithBlob(func(blob *sqlite.Blob) (err error) {
		n, err = blobWriteAt(blob, b, off)
		return
	}, true, false)
	return
}

func (p Blob) SetTag(name string, value interface{}) (err error) {
	p.cache.l.Lock()
	defer p.cache.l.Unlock()
	err = p.cache.getCacheErr()
	if err != nil {
		return
	}
	return sqlitex.Exec(p.cache.conn, "insert or replace into tag (blob_name, tag_name, value) values (?, ?, ?)", nil,
		p.name, name, value)
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

func (c *Cache) getBlob(name string, create bool, length int64, clobberLength bool) (_ *sqlite.Blob, err error) {
	blob, ok := c.blobs[name]
	if ok {
		if !clobberLength || length == blob.Size() {
			return blob, nil
		}
		blob.Close()
		delete(c.blobs, name)
	}
	rowid, existingLength, ok, err := rowidForBlob(c.conn, name)
	// log.Printf("got rowid %v, existing length %v, ok %v", rowid, existingLength, ok)
	if err != nil {
		return nil, fmt.Errorf("getting rowid for blob: %w", err)
	}
	if !ok && !create {
		return nil, fs.ErrNotExist
	}
	if !ok || (clobberLength && existingLength != length) {
		rowid, err = createBlob(c.conn, name, length, ok && clobberLength && length != existingLength)
		if err != nil {
			err = fmt.Errorf("creating blob: %w", err)
			return nil, err
		}
	}
	blob, err = c.conn.OpenBlob("main", "blob_data", "data", rowid, true)
	if err != nil {
		panic(err)
	}
	if c.opts.GcBlobs {
		herp := new(byte)
		runtime.SetFinalizer(herp, func(*byte) {
			c.l.Lock()
			defer c.l.Unlock()
			// Note there's no guarantee that the finalizer fired while this blob is the same
			// one in the blob cache. It might be possible to rework this so that we check, or
			// strip finalizers as appropriate.
			blob.Close()
		})
	}
	c.blobs[name] = blob
	return blob, nil
}

func (b Blob) Delete() {
	b.cache.l.Lock()
	defer b.cache.l.Unlock()
	sqlitex.Execute(b.cache.conn, "delete from blob where name=?", &sqlitex.ExecOptions{
		Args: []interface{}{b.name},
	})
}
