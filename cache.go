package squirrel

import (
	"sync"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
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

func (cl *Cache) GetCapacity() (ret int64, ok bool) {
	cl.l.Lock()
	defer cl.l.Unlock()
	err := sqlitex.Exec(cl.conn, "select value from setting where name='capacity'", func(stmt *sqlite.Stmt) error {
		ok = true
		ret = stmt.ColumnInt64(0)
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

// Returns an existing blob only.
func (c *Cache) Open(name string) (ret PinnedBlob, err error) {
	ret.c = c
	c.l.Lock()
	defer c.l.Unlock()
	ret.sb, err = c.getBlob(name, false, -1, false)
	return
}

func (c *Cache) OpenWithLength(name string, length int64) Blob {
	return Blob{
		name:   name,
		length: length,
		cache:  c,
	}
}

func (c *Cache) Put(name string, b []byte) error {
	return Blob{
		name:   name,
		length: int64(len(b)),
		cache:  c,
	}.doWithBlob(func(blob *sqlite.Blob) error {
		_, err := blobWriteAt(blob, b, 0)
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
