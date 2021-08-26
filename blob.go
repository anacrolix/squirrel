package squirrel

import (
	"errors"
	"fmt"
	"io/fs"
	"runtime"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
)

type Blob struct {
	Name   string
	Length int64
	Cache  *Cache
}

func (p Blob) doWithBlob(
	withBlob func(*sqlite.Blob) error,
	create bool,
	clobberLength bool,
) (err error) {
	p.Cache.l.Lock()
	defer p.Cache.l.Unlock()
	if p.Cache.opts.NoCacheBlobs {
		defer p.forgetBlob()
	}
	blob, err := p.getBlob(create, clobberLength)
	if err != nil {
		err = fmt.Errorf("getting sqlite blob: %w", err)
		return
	}
	err = withBlob(blob)
	if err == nil {
		return
	}
	var se sqlite.Error
	if !errors.As(err, &se) {
		return
	}
	// "ABORT" occurs if the row the blob is on is modified elsewhere. "ERROR: invalid blob" occurs
	// if the blob has been closed. We don't forget blobs that are closed by our GC finalizers,
	// because they may be attached to names that have since moved on to another blob.
	if se.Code != sqlite.SQLITE_ABORT && !(p.Cache.opts.GcBlobs && se.Code == sqlite.SQLITE_ERROR && se.Msg == "invalid blob") {
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
		n, err = blob.ReadAt(b, off)
		return
	}, false, false)
	return
}

func (p Blob) WriteAt(b []byte, off int64) (n int, err error) {
	err = p.doWithBlob(func(blob *sqlite.Blob) (err error) {
		n, err = blob.WriteAt(b, off)
		var se sqlite.Error
		if errors.As(err, &se) && se.Code == sqlite.SQLITE_ERROR && off+int64(len(b)) > blob.Size() {
			err = fmt.Errorf("write would be out of bounds: %w", err)
		}
		return
	}, true, false)
	return
}

func (p Blob) SetTag(name string, value interface{}) error {
	p.Cache.l.Lock()
	defer p.Cache.l.Unlock()
	return sqlitex.Exec(p.Cache.conn, "insert or replace into tag (blob_name, tag_name, value) values (?, ?, ?)", nil,
		p.Name, name, value)
}

func (p Blob) forgetBlob() {
	blob, ok := p.Cache.blobs[p.Name]
	if !ok {
		return
	}
	blob.Close()
	delete(p.Cache.blobs, p.Name)
}

func (p Blob) GetTag(name string, result func(*sqlite.Stmt)) error {
	p.Cache.l.Lock()
	defer p.Cache.l.Unlock()
	return sqlitex.Exec(p.Cache.conn, "select value from tag where blob_name=? and tag_name=?", func(stmt *sqlite.Stmt) error {
		result(stmt)
		return nil
	}, p.Name, name)
}

func (p Blob) getBlob(create bool, clobberLength bool) (*sqlite.Blob, error) {
	blob, ok := p.Cache.blobs[p.Name]
	if ok {
		if !clobberLength || p.Length == blob.Size() {
			return blob, nil
		}
		blob.Close()
		delete(p.Cache.blobs, p.Name)
	}
	rowid, length, ok, err := rowidForBlob(p.Cache.conn, p.Name)
	if err != nil {
		return nil, fmt.Errorf("getting rowid for blob: %w", err)
	}
	if !ok && !create {
		return nil, fs.ErrNotExist
	}
	if !ok || clobberLength && length != p.Length {
		rowid, err = createBlob(p.Cache.conn, p.Name, p.Length, ok && clobberLength && length != p.Length)
	}
	if err != nil {
		err = fmt.Errorf("creating blob: %w", err)
		return nil, err
	}
	blob, err = p.Cache.conn.OpenBlob("main", "blob", "data", rowid, true)
	if err != nil {
		panic(err)
	}
	if p.Cache.opts.GcBlobs {
		herp := new(byte)
		runtime.SetFinalizer(herp, func(*byte) {
			p.Cache.l.Lock()
			defer p.Cache.l.Unlock()
			// Note there's no guarantee that the finalizer fired while this blob is the same
			// one in the blob cache. It might be possible to rework this so that we check, or
			// strip finalizers as appropriate.
			blob.Close()
		})
	}
	p.Cache.blobs[p.Name] = blob
	return blob, nil
}
