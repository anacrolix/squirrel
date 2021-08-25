//go:build cgo
// +build cgo

package squirrel

import (
	"errors"
	"fmt"
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

func rowidForBlob(c conn, name string, length int64, create bool) (rowid int64, err error) {
	rowidOk := false
	err = sqlitex.Exec(c, "select rowid from blob where name=?", func(stmt *sqlite.Stmt) error {
		if rowidOk {
			panic("expected at most one row")
		}
		// TODO: How do we know if we got this wrong?
		rowid = stmt.ColumnInt64(0)
		rowidOk = true
		return nil
	}, name)
	if err != nil {
		return
	}
	if rowidOk {
		return
	}
	if !create {
		err = errors.New("no existing row")
		return
	}
	err = sqlitex.Exec(c, "insert into blob(name, data) values(?, zeroblob(?))", nil, name, length)
	if err != nil {
		return
	}
	rowid = c.LastInsertRowID()
	return
}

type Blob struct {
	Name   string
	Length int64
	Cache  *Cache
}

func (p Blob) doAtIoWithBlob(
	atIo func(*sqlite.Blob) func([]byte, int64) (int, error),
	b []byte,
	off int64,
	create bool,
) (n int, err error) {
	p.Cache.l.Lock()
	defer p.Cache.l.Unlock()
	if p.Cache.opts.NoCacheBlobs {
		defer p.forgetBlob()
	}
	blob, err := p.getBlob(create)
	if err != nil {
		err = fmt.Errorf("getting blob: %w", err)
		return
	}
	n, err = atIo(blob)(b, off)
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
	blob, err = p.getBlob(create)
	if err != nil {
		err = fmt.Errorf("getting blob: %w", err)
		return
	}
	return atIo(blob)(b, off)
}

func (p Blob) ReadAt(b []byte, off int64) (n int, err error) {
	return p.doAtIoWithBlob(func(blob *sqlite.Blob) func([]byte, int64) (int, error) {
		return blob.ReadAt
	}, b, off, false)
}

func (p Blob) WriteAt(b []byte, off int64) (n int, err error) {
	return p.doAtIoWithBlob(func(blob *sqlite.Blob) func([]byte, int64) (int, error) {
		return blob.WriteAt
	}, b, off, true)
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

func (p Blob) getBlob(create bool) (*sqlite.Blob, error) {
	blob, ok := p.Cache.blobs[p.Name]
	if !ok {
		rowid, err := rowidForBlob(p.Cache.conn, p.Name, p.Length, create)
		if err != nil {
			return nil, fmt.Errorf("getting rowid for blob: %w", err)
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
	}
	return blob, nil
}
