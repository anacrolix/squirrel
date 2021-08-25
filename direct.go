//go:build cgo
// +build cgo

package squirrel

import (
	"errors"
	"fmt"
	"io/fs"
	"runtime"
	"sync"
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
)

type NewCacheOpts struct {
	NewConnOpts
	InitDbOpts
	InitConnOpts
	GcBlobs           bool
	NoCacheBlobs      bool
	BlobFlushInterval time.Duration
}

func NewCache(opts NewCacheOpts) (_ *Cache, err error) {
	conn, err := newConn(opts.NewConnOpts)
	if err != nil {
		return
	}
	if opts.PageSize == 0 {
		// The largest size sqlite supports. I think we want this to be the smallest Blob size we
		// can expect, which is probably 1<<17.
		opts.PageSize = 1 << 16
	}
	err = initDatabase(conn, opts.InitDbOpts)
	if err != nil {
		conn.Close()
		return
	}
	err = initConn(conn, opts.InitConnOpts)
	if err != nil {
		conn.Close()
		return
	}
	if opts.BlobFlushInterval == 0 && !opts.GcBlobs {
		// This is influenced by typical busy timeouts, of 5-10s. We want to give other connections
		// a few chances at getting a transaction through.
		opts.BlobFlushInterval = time.Second
	}
	cl := &Cache{
		conn:  conn,
		blobs: make(map[string]*sqlite.Blob),
		opts:  opts,
	}
	// Avoid race with cl.blobFlusherFunc
	cl.l.Lock()
	defer cl.l.Unlock()
	if opts.BlobFlushInterval != 0 {
		cl.blobFlusher = time.AfterFunc(opts.BlobFlushInterval, cl.blobFlusherFunc)
	}
	return cl, nil
}

func (cl *Cache) GetCapacity() (ret *int64) {
	cl.l.Lock()
	defer cl.l.Unlock()
	err := sqlitex.Exec(cl.conn, "select value from setting where name='capacity'", func(stmt *sqlite.Stmt) error {
		ret = new(int64)
		*ret = stmt.ColumnInt64(0)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return
}

type Cache struct {
	l           sync.Mutex
	conn        conn
	blobs       map[string]*sqlite.Blob
	blobFlusher *time.Timer
	opts        NewCacheOpts
	closed      bool
}

func (c *Cache) blobFlusherFunc() {
	c.l.Lock()
	defer c.l.Unlock()
	c.flushBlobs()
	if !c.closed {
		c.blobFlusher.Reset(c.opts.BlobFlushInterval)
	}
}

func (c *Cache) flushBlobs() {
	for key, b := range c.blobs {
		// Need the lock to prevent racing with the GC finalizers.
		b.Close()
		delete(c.blobs, key)
	}
}

func (c *Cache) Close() (err error) {
	c.l.Lock()
	defer c.l.Unlock()
	c.flushBlobs()
	if c.opts.BlobFlushInterval != 0 {
		c.blobFlusher.Stop()
	}
	if !c.closed {
		c.closed = true
		err = c.conn.Close()
		c.conn = nil
	}
	return
}

func (c *Cache) Open(name string) (Blob, error) {
	b := Blob{
		Name:  name,
		Cache: c,
	}
	err := b.doWithBlob(func(blob *sqlite.Blob) error {
		b.Length = blob.Size()
		return nil
	}, false, false)
	return b, err
}

func (c *Cache) Put(name string, b []byte) error {
	return Blob{
		Name:   name,
		Length: int64(len(b)),
		Cache:  c,
	}.doWithBlob(func(blob *sqlite.Blob) error {
		_, err := blob.WriteAt(b, 0)
		return err
	}, true, true)
}

func createBlob(c conn, name string, length int64, clobber bool) (rowid int64, err error) {
	query := func() string {
		if clobber {
			return "insert or replace into blob(name, data) values(?, zeroblob(?))"
		} else {
			return "insert into blob(name, data) values(?, zeroblob(?))"
		}
	}()
	err = sqlitex.Exec(c, query, nil, name, length)
	rowid = c.LastInsertRowID()
	return
}

func rowidForBlob(c conn, name string) (rowid int64, length int64, ok bool, err error) {
	err = sqlitex.Exec(c, "select rowid, length(data) from blob where name=?", func(stmt *sqlite.Stmt) error {
		if ok {
			panic("expected at most one row")
		}
		// TODO: How do we know if we got this wrong?
		rowid = stmt.ColumnInt64(0)
		length = stmt.ColumnInt64(1)
		ok = true
		return nil
	}, name)
	if err != nil {
		return
	}
	return
}

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
