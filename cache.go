package squirrel

import (
	"errors"
	"fmt"
	"github.com/anacrolix/log"
	"github.com/anacrolix/sync"
	"time"

	"github.com/ajwerner/btree"

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
	Logger            log.Logger
}

func newConn(opts NewCacheOpts) (ret conn, err error) {
	conn, err := newSqliteConn(opts.NewConnOpts)
	if err != nil {
		return
	}
	ret = new(connStruct)
	ret.sqliteConn = conn
	ret.blobs = makeBlobCache()
	ret.maxBlobSize = opts.MaxBlobSize.UnwrapOr(defaultMaxBlobSize)
	ret.logger = opts.Logger
	err = initConn(ret, opts)
	if err != nil {
		err = errors.Join(err, ret.Close())
	}
	return
}

// Inits sqlite for a conn.
func initConn(conn conn, opts NewCacheOpts) (err error) {
	if opts.ConnBlockedOnBusy != nil {
		returned := make(chan struct{})
		defer close(returned)
		go func() {
			select {
			case <-conn.sqliteConn.BlockedOnBusy.On():
				close(*opts.ConnBlockedOnBusy)
			case <-returned:
			}
		}()
	}
	err = sqlitex.ExecTransient(conn.sqliteConn, "pragma foreign_keys=on", nil)
	if err != nil {
		return
	}
	err = setSynchronous(conn.sqliteConn, opts.SetSynchronous)
	if err != nil {
		return
	}
	// For some reason it's faster to set page size after synchronous. We need to set it before
	// setting journal mode in case it's WAL.
	err = setPageSize(conn.sqliteConn, opts.PageSize)
	if err != nil {
		err = fmt.Errorf("setting page size: %w", err)
		return
	}
	// pragma auto_vacuum=X needs to occur before pragma journal_mode=wal
	err = initDatabase(conn.sqliteConn, opts.InitDbOpts)
	if err != nil {
		return
	}
	err = initSqliteConn(conn.sqliteConn, opts.InitConnOpts, opts.PageSize)
	if err != nil {
		return
	}
	err = conn.trimToCapacity(nil)
	if err != nil {
		return
	}
	if opts.MaxPageCount.Ok {
		err = setAndVerifyPragma(conn.sqliteConn, "max_page_count", opts.MaxPageCount.Value)
		if err != nil {
			return
		}
	}
	return
}

func makeBlobCache() btree.Map[valueKey, *sqlite.Blob] {
	return btree.MakeMap[valueKey, *sqlite.Blob](func(l, r valueKey) int {
		if l.keyId != r.keyId {
			if l.keyId < r.keyId {
				return -1
			} else {
				return 1
			}
		}
		if l.offset != r.offset {
			if l.offset < r.offset {
				return -1
			} else {
				return 1
			}
		}
		return 0
	})
}

func NewCache(opts NewCacheOpts) (_ *Cache, err error) {
	cl := &Cache{
		opts: opts,
	}
	if cl.opts.Logger.IsZero() {
		cl.opts.Logger = log.Default
	}
	cl.closeCond.L = &cl.l
	conn, err := cl.newConn()
	if err != nil {
		return
	}
	cl.addConn(conn)
	return cl, nil
}

func (cl *Cache) newConn() (conn, error) {
	return newConn(cl.opts)
}

func (cl *Cache) addConn(conn conn) {
	cl.conns = append(cl.conns, conn)
}

func (cl *Cache) GetCapacity() (ret int64, ok bool) {
	err := cl.execWithConn(
		"select value from setting where name='capacity'",
		func(stmt *sqlite.Stmt) error {
			ok = true
			ret = stmt.ColumnInt64(0)
			return nil
		},
	)
	if err != nil {
		panic(err)
	}
	return
}

func (cl *Cache) popConn() (ret conn) {
	ret = cl.conns[len(cl.conns)-1]
	cl.conns = cl.conns[:len(cl.conns)-1]
	return
}

func (cl *Cache) pushConn(conn conn) {
	cl.conns = append(cl.conns, conn)
}

func (cl *Cache) withConn(with func(conn) error) (err error) {
	cl.l.Lock()
	if len(cl.conns) == 0 {
		cl.l.Unlock()
		var conn conn
		conn, err = cl.newConn()
		if err != nil {
			return
		}
		cl.l.Lock()
		cl.addConn(conn)
	}
	conn := cl.popConn()
	cl.connsInUse++
	cl.l.Unlock()
	err = with(conn)
	cl.l.Lock()
	cl.pushConn(conn)
	cl.connsInUse--
	cl.closeCond.Broadcast()
	cl.l.Unlock()
	return
}

func (cl *Cache) execWithConn(query string, result func(stmt *sqlite.Stmt) error) (err error) {
	return cl.withConn(func(c conn) error {
		return sqlitex.Exec(c.sqliteConn, query, result)
	})
}

type Cache struct {
	l          sync.RWMutex
	conns      []conn
	connsInUse int
	opts       NewCacheOpts
	closeCond  sync.Cond
	closed     bool
	// Anytime we know that we have to write to the sqlite conn, we should try to synchronize on a
	// single connection for cache re-use and to minimize busy waits on multiple connections.
	singleWriter sync.Mutex
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
		for {
			for len(c.conns) != 0 {
				err = errors.Join(err, c.popConn().Close())
			}
			if c.connsInUse == 0 {
				break
			}
			c.closeCond.Wait()
		}
	}
	return
}

// Returns a PinnedBlob. The item must already exist. You must call PinnedBlob.Close when done
// with it.
func (c *Cache) OpenPinnedReadOnly(name string) (ret CachePinnedBlob, err error) {
	return c.getPinnedBlob(
		c.Tx,
		func(tx *Tx) (*PinnedBlob, error) {
			return tx.OpenPinnedReadOnly(name)
		})
}

// Returns a PinnedBlob with its own implied Tx.
func (c *Cache) Create(name string, opts CreateOpts) (ret CachePinnedBlob, err error) {
	return c.getPinnedBlob(
		c.TxImmediate,
		func(tx *Tx) (*PinnedBlob, error) {
			return tx.Create(name, opts)
		})
}

// Returns a PinnedBlob with an automatic Tx. The Tx is closed when the returned value is Closed.
func (c *Cache) getPinnedBlob(
	getTx func(f func(tx *Tx) error) error,
	fromTx func(tx *Tx) (*PinnedBlob, error),
) (ret CachePinnedBlob, err error) {
	ready := make(chan struct{})
	ret.txFinished = make(chan struct{})
	closed := false
	go func() {
		defer close(ret.txFinished)
		err = getTx(func(tx *Tx) (err error) {
			pb, err := fromTx(tx)
			if err != nil {
				return
			}
			finishTx := make(chan struct{})
			ret.finishTx = sync.OnceFunc(func() {
				close(finishTx)
			})
			ret.PinnedBlob = pb
			close(ready)
			closed = true
			<-finishTx
			return nil
		})
		if err != nil {
			if closed {
				panic(err)
			}
			closed = true
			close(ready)
		}
	}()
	<-ready
	return
}

type CachePinnedBlob struct {
	// An item that exists inside its own transaction.
	*PinnedBlob
	// Call this to end the transaction for the above item.
	finishTx func()
	// This is closed when the transaction has returned. If you don't wait you can rush into a new
	// transaction with a Cache, not see conn returned, and create a new one that gets SQLITE_BUSY
	// when it tries to upgrade to write.
	txFinished chan struct{}
}

func (me CachePinnedBlob) Close() (err error) {
	err = me.PinnedBlob.Close()
	me.finishTx()
	<-me.txFinished
	return
}

// Returns a PinnedBlob. The item must already exist. You must call PinnedBlob.Close when done
// with it.
func (tx *Tx) openPinned(name string, write bool) (ret *PinnedBlob, err error) {
	valueId, err := tx.conn.getValueIdForKey(name)
	if err != nil {
		return
	}
	ret = &PinnedBlob{
		key:     name,
		tx:      tx,
		write:   write,
		valueId: valueId,
	}
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

func (c *Cache) ReadFull(key string, b []byte) (n int, err error) {
	err = c.wrapTxMethod(func(tx *Tx) error {
		n, err = tx.ReadFull(key, b)
		return err
	})
	return
}

func (c *Cache) ReadAll(key string, b []byte) (ret []byte, err error) {
	err = c.wrapTxMethod(func(tx *Tx) error {
		ret, err = tx.ReadAll(key, b)
		return err
	})
	return
}

func (c *Cache) runTx(f func(tx *Tx) error, level string) (err error) {
	err = c.withConn(func(c conn) (err error) {
		err = sqlitex.Exec(c.sqliteConn, "begin "+level, nil)
		if err != nil {
			return
		}
		tx := Tx{
			conn:  c,
			write: level != "",
		}
		err = f(&tx)
		c.closeBlobs()
		// TODO: Only trim when added to the database, or know that we upgraded to a write transaction already?
		if err == nil {
			err = c.trimToCapacity(func(key rowid) {
				delete(tx.accessedKeys, key)
			})
		}
		if err == nil {
			for keyId := range tx.accessedKeys {
				var ignored bool
				ignored, err = c.accessedKey(keyId, !tx.write)
				if err != nil || ignored {
					break
				}
			}
		}
		if err == nil {
			err = sqlitex.Exec(c.sqliteConn, "commit", nil)
			return
		}
		// Autocommit is re-enabled if a transaction is automatically rolled back such as by SQLITE_FULL.
		if !c.sqliteConn.GetAutocommit() {
			rollbackErr := sqlitex.Exec(c.sqliteConn, "rollback", nil)
			if rollbackErr != nil {
				err = errors.Join(err, rollbackErr)
			}
		}
		return
	})
	return
}

func (c *Cache) Tx(f func(tx *Tx) error) (err error) {
	return c.runTx(f, "")
}

func (c *Cache) TxImmediate(f func(tx *Tx) error) (err error) {
	c.singleWriter.Lock()
	defer c.singleWriter.Unlock()
	return c.runTx(f, "immediate")
}

func (c *Cache) SetTag(key, name string, value interface{}) (err error) {
	return c.TxImmediate(func(tx *Tx) error {
		return tx.SetTag(key, name, value)
	})
}

func (c *Cache) wrapTxMethod(txCall func(tx *Tx) error) error {
	return c.Tx(func(tx *Tx) error {
		return txCall(tx)
	})
}

func openSqliteBlob(sc sqliteConn, rowid rowid, write bool) (*sqlite.Blob, error) {
	return sc.OpenBlob("main", "blobs", "blob", rowid, write)
}

func timeFromStmtColumn(stmt *sqlite.Stmt, col int) time.Time {
	unixMs := stmt.ColumnInt64(col)
	return time.UnixMilli(unixMs)
}

func (c conn) lastUsed(rowid rowid) (lastUsed time.Time, err error) {
	ok, err := c.sqliteQueryRow(
		`select last_used from keys where key_id=?`,
		func(stmt *sqlite.Stmt) error {
			lastUsed = timeFromStmtColumn(stmt, 0)
			return nil
		},
		rowid,
	)
	if err != nil {
		return
	}
	if !ok {
		err = ErrNotFound
	}
	return
}

func (c conn) lastUsedByKey(key string) (lastUsed time.Time, err error) {
	ok, err := c.sqliteQueryRow(
		`select last_used from keys where key=?`,
		func(stmt *sqlite.Stmt) error {
			lastUsed = timeFromStmtColumn(stmt, 0)
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
