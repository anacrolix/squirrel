package squirrel

import (
	"errors"
	"fmt"
	"sync"
	"time"

	g "github.com/anacrolix/generics"
	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

type NewCacheOpts struct {
	NewConnOpts
	InitDbOpts
	InitConnOpts
	// If not-nil, this will be closed if the sqlite busy handler is invoked while initializing the
	// Cache.
	ConnBlockedOnBusy *chan struct{}
}

func NewCache(opts NewCacheOpts) (_ *Cache, err error) {
	conn, err := newConn(opts.NewConnOpts)
	if err != nil {
		return
	}
	if opts.ConnBlockedOnBusy != nil {
		returned := make(chan struct{})
		defer close(returned)
		go func() {
			select {
			case <-conn.BlockedOnBusy.On():
				close(*opts.ConnBlockedOnBusy)
			case <-returned:
			}
		}()
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
		conn: conn,
		opts: opts,
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
	l      sync.RWMutex
	conn   conn
	opts   NewCacheOpts
	closed bool
	tx     Tx
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
	if !c.closed {
		c.closed = true
		err = c.conn.Close()
		c.conn = nil
	}
	return
}

// Returns a PinnedBlob. The item must already exist. You must call PinnedBlob.Close when done
// with it.
func (c *Cache) OpenPinnedReadOnly(name string) (ret *PinnedBlob, err error) {
	c.l.Lock()
	defer c.l.Unlock()
	err = c.getCacheErr()
	if err != nil {
		return
	}
	return c.openPinned(name, false, nil)
}

// Returns a PinnedBlob. The item must already exist. You must call PinnedBlob.Close when done
// with it.
func (c *Cache) openPinned(name string, write bool, tx *Tx) (ret *PinnedBlob, err error) {
	ret = &PinnedBlob{
		key:   name,
		c:     c,
		tx:    tx,
		write: write,
	}
	ret.blob, ret.rowid, err = c.getBlob(name, false, -1, false, g.None[int64](), write)
	return
}

// Defines a Blob with the given name and length. Nothing is actually written or checked in the DB.
func (c *Cache) NewBlobRef(name string) Blob {
	return Blob{
		name:  name,
		cache: c,
	}
}

// Defines a Blob with the given name and length. Nothing is actually written or checked in the DB.
func (c *Cache) BlobWithLength(name string, length int64) Blob {
	return Blob{
		name:   name,
		length: g.Some(length),
		cache:  c,
	}
}

// Deprecated. Use BlobWithLength.
func (c *Cache) OpenWithLength(name string, length int64) Blob {
	return c.BlobWithLength(name, length)
}

func (c *Cache) Put(name string, b []byte) (err error) {
	txErr := c.TxImmediate(func(tx *Tx) error {
		return tx.Put(name, b)
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
		    	last_used=cast(unixepoch('subsec')*1e3 as integer),
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

func (c *Cache) runTx(f func(tx *Tx) error, level string) (err error) {
	c.l.Lock()
	defer c.l.Unlock()
	err = c.getCacheErr()
	if err != nil {
		return
	}
	err = sqlitex.Exec(c.conn, "begin "+level, nil)
	if err != nil {
		return
	}
	err = f(&c.tx)
	if err == nil {
		err = sqlitex.Exec(c.conn, "commit", nil)
	} else {
		err = errors.Join(err, sqlitex.Exec(c.conn, "rollback", nil))
	}
	return
}

func (c *Cache) Tx(f func(tx *Tx) error) (err error) {
	return c.runTx(f, "")
}

func (c *Cache) TxImmediate(f func(tx *Tx) error) (err error) {
	return c.runTx(f, "immediate")
}

func (c *Cache) SetTag(key, name string, value interface{}) (err error) {
	return c.wrapTxMethod(func(tx *Tx) error {
		return tx.SetTag(key, name, value)
	})
}

func (c *Cache) wrapTxMethod(txCall func(tx *Tx) error) error {
	return c.Tx(func(tx *Tx) error {
		return txCall(tx)
	})
}

func (c *Cache) openBlob(rowid int64, write bool) (*sqlite.Blob, error) {
	return c.conn.OpenBlob("main", "blob_data", "data", rowid, write)
}

func (c *Cache) getBlob(
	name string,
	create bool,
	length int64,
	clobberLength bool,
	rowidHint g.Option[int64],
	write bool,
) (blob *sqlite.Blob, rowid rowid, err error) {
	if rowidHint.Ok {
		blob, err = c.openBlob(rowidHint.Value, write)
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
			err = ErrNotFound
			return
		}
		if !ok || (clobberLength && existingLength != length) {
			rowid, err = createBlob(c.conn, name, length, ok && clobberLength && length != existingLength)
			if err != nil {
				err = fmt.Errorf("creating blob: %w", err)
				return
			}
		}
		blob, err = c.openBlob(rowid, write)
		if err != nil {
			panic(err)
		}
	}
	return blob, rowid, nil
}

func (c *Cache) lastUsed(key string) (lastUsed time.Time, err error) {
	var unixMs setOnce[int64]
	err = sqlitex.Exec(
		c.conn,
		`select last_used from blob where name=?`,
		func(stmt *sqlite.Stmt) error {
			unixMs.Set(stmt.ColumnInt64(0))
			lastUsed = time.UnixMilli(unixMs.value)
			return nil
		},
		key,
	)
	if !unixMs.ok {
		err = ErrNotFound
	}
	return
}
