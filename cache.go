package squirrel

import (
	"errors"
	g "github.com/anacrolix/generics"
	"sync"
	"time"

	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

type NewCacheOpts struct {
	NewConnOpts
	InitDbOpts
	InitConnOpts
}

func NewCache(opts NewCacheOpts) (_ *Cache, err error) {
	conn, err := newConn(opts.NewConnOpts)
	if err != nil {
		return
	}
	// pragma auto_vacuum=X needs to occur before pragma journal_mode=wal
	err = initDatabase(conn, opts.InitDbOpts)
	if err != nil {
		conn.Close()
		return
	}
	err = initConn(conn, opts.InitConnOpts, opts.PageSize)
	if err != nil {
		conn.Close()
		return
	}
	cl := &Cache{
		conn:  conn,
		blobs: make(map[string]*sqlite.Blob),
		opts:  opts,
	}
	cl.tx.c = cl
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
	tx          Tx
}

func (c *Cache) getCacheErr() error {
	if c.closed {
		return errors.New("cache closed")
	}
	return nil
}

func (c *Cache) Close() (err error) {
	c.l.Lock()
	defer c.l.Unlock()
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

// Returns a PinnedBlob. The item must already exist. You must call PinnedBlob.Release when done
// with it.
func (c *Cache) OpenPinned(name string) (ret *PinnedBlob, err error) {
	c.l.Lock()
	defer c.l.Unlock()
	err = c.getCacheErr()
	if err != nil {
		return
	}
	ret = &PinnedBlob{
		key: name,
		c:   c,
	}
	ret.c = c
	ret.key = name
	ret.blob, ret.rowid, err = c.getBlob(name, false, -1, false, g.None[int64]())
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

func (c *Cache) Put(name string, b []byte) (err error) {
	txErr := c.Tx(func(tx *Tx) bool {
		err = tx.Put(name, b)
		return err == nil
	})
	return errors.Join(err, txErr)
}

var ErrNotFound = errors.New("not found")

func (c *Cache) accessBlob(key string) (dataId rowid, err error) {
	var blobDataId setOnce[rowid]
	err = sqlitex.Exec(
		c.conn, `
		update blob
			set 
		    	last_used=datetime('now', 'subsec'),
		    	access_count=access_count+1
			where name=?
			returning data_id`,
		func(stmt *sqlite.Stmt) error {
			blobDataId.Set(stmt.ColumnInt64(0))
			return nil
		},
		key,
	)
	if err != nil {
		return
	}
	if !blobDataId.Ok() {
		err = ErrNotFound
	}
	dataId = blobDataId.value
	return
}

func (c *Cache) ReadFull(key string, b []byte) (n int, err error) {
	err = c.wrapTxMethod(func(tx *Tx) error {
		n, err = tx.ReadFull(key, b)
		return err
	})
	return
}

func (c *Cache) Tx(f func(tx *Tx) bool) (err error) {
	c.l.Lock()
	defer c.l.Unlock()
	err = c.getCacheErr()
	if err != nil {
		return
	}
	err = sqlitex.Exec(c.conn, "begin immediate", nil)
	if err != nil {
		return
	}
	commit := f(&c.tx)
	if commit {
		err = sqlitex.Exec(c.conn, "commit", nil)
	} else {
		err = sqlitex.Exec(c.conn, "rollback", nil)
	}
	return
}

func (c *Cache) SetTag(key, name string, value interface{}) (err error) {
	return c.wrapTxMethod(func(tx *Tx) error {
		return tx.SetTag(key, name, value)
	})
}

func (c *Cache) wrapTxMethod(txCall func(tx *Tx) error) error {
	var methodErr error
	txErr := c.Tx(func(tx *Tx) bool {
		methodErr = txCall(tx)
		return methodErr == nil
	})
	return errors.Join(txErr, methodErr)
}
