package squirrel

import (
	"errors"
	"sync"
	"time"

	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

type NewCacheOpts struct {
	NewConnOpts
	InitDbOpts
	InitConnOpts
	GcBlobs           bool
	NoCacheBlobs      bool
	BlobFlushInterval time.Duration
	NoFlushBlobs      bool
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
	err = initConn(conn, opts.InitConnOpts, opts.PageSize)
	if err != nil {
		conn.Close()
		return
	}
	err = initDatabase(conn, opts.InitDbOpts)
	if err != nil {
		conn.Close()
		return
	}
	if opts.BlobFlushInterval == 0 {
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
	if !opts.NoFlushBlobs && !opts.GcBlobs {
		cl.blobFlusher = time.AfterFunc(opts.BlobFlushInterval, cl.blobFlusherFunc)
	}
	return cl, nil
}

func (cl *Cache) GetCapacity() (ret int64, ok bool) {
	cl.l.Lock()
	defer cl.l.Unlock()
	if cl.getCacheErr() != nil {
		return
	}
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

func (c *Cache) reclaimsBlobs() bool {
	return !c.opts.NoCacheBlobs
}

func (c *Cache) getCacheErr() error {
	if c.closed {
		return errors.New("cache closed")
	}
	return nil
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
	if c.blobFlusher != nil {
		c.blobFlusher.Stop()
	}
	if !c.closed {
		c.closed = true
		err = c.conn.Close()
		c.conn = nil
	}
	return
}

// Returns an existing blob only.
func (c *Cache) Open(name string) (ret PinnedBlob, err error) {
	if !c.reclaimsBlobs() {
		err = errors.New("you must call OpenPinned if blob caching is disabled")
		return
	}
	return c.OpenPinned(name)
}

// Returns a PinnedBlob. The item must already exist. You must call PinnedBlob.Release when done
// with it.
func (c *Cache) OpenPinned(name string) (ret PinnedBlob, err error) {
	ret.c = c
	c.l.Lock()
	defer c.l.Unlock()
	err = c.getCacheErr()
	if err != nil {
		return
	}
	ret.blob, err = c.getBlob(name, false, -1, false)
	return
}

// Defines a Blob with the given name and length. Nothing is actually written or checked in the DB.
func (c *Cache) BlobWithLength(name string, length int64) Blob {
	return Blob{
		name:   name,
		length: length,
		cache:  c,
	}
}

// Deprecated. Use BlobWithLength.
func (c *Cache) OpenWithLength(name string, length int64) Blob {
	return c.BlobWithLength(name, length)
}

func (c *Cache) Put(name string, b []byte) error {
	return Blob{
		name:   name,
		length: int64(len(b)),
		cache:  c,
	}.doWithBlob(func(blob *sqlite.Blob) error {
		_, err := blobWriteAt(blob, b, 0)
		// log.Printf("wrote %v bytes", n)
		return err
	}, true, true)
}

var ErrNotFound = errors.New("not found")

func (c *Cache) ReadFull(key string, b []byte) (n int, err error) {
	c.l.Lock()
	defer c.l.Unlock()
	ok := false
	err = sqlitex.Exec(
		c.conn,
		`select data from blob join blob_data using (data_id) where name=?`,
		func(stmt *sqlite.Stmt) error {
			if ok {
				panic("duplicate rows for key")
			}
			colBytes := stmt.ColumnViewBytes(0)
			n = copy(b, colBytes)
			ok = true
			return nil
		},
		key,
	)
	if err != nil {
		return
	}
	if !ok {
		err = ErrNotFound
	}
	return
}

func (c *Cache) Tx(f func() bool) (err error) {
	err = sqlitex.Exec(c.conn, "begin immediate", nil)
	if err != nil {
		return
	}
	commit := f()
	if commit {
		err = sqlitex.Exec(c.conn, "commit", nil)
	} else {
		err = sqlitex.Exec(c.conn, "rollback", nil)
	}
	return
}
