package squirrel

import (
	"time"

	g "github.com/anacrolix/generics"
	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

// Blobs are references to a name in a Cache that are looked up when its methods are used. They
// should not be when inside a Tx.
type Blob struct {
	name   string
	length g.Option[int64]
	cache  *Cache
}

func (p Blob) ReadAt(b []byte, off int64) (n int, err error) {
	err = p.cache.Tx(func(tx *Tx) (err error) {
		pb, err := tx.OpenPinnedReadOnly(p.name)
		if err != nil {
			return
		}
		defer pb.Close()
		n, err = pb.ReadAt(b, off)
		return
	})
	return
}

func (p Blob) WriteAt(b []byte, off int64) (n int, err error) {
	err = p.cache.TxImmediate(func(tx *Tx) (err error) {
		pb, err := tx.Create(p.name, CreateOpts{p.length.Unwrap()})
		if err != nil {
			return
		}
		defer pb.Close()
		n, err = pb.WriteAt(b, off)
		return
	})
	return
}

func (p Blob) SetTag(name string, value interface{}) (err error) {
	return p.cache.SetTag(p.name, name, value)
}

type SqliteStmt = *sqlite.Stmt

func (p Blob) GetTag(name string, result func(stmt SqliteStmt)) error {
	return p.cache.withConn(func(c conn) error {
		return c.sqliteQueryMaxOneRow(
			`select value from tags join keys using (key_id) where key=? and tag_name=?`,
			func(stmt SqliteStmt) error {
				result(stmt)
				return nil
			},
			p.name, name,
		)
	})
}

func (b Blob) Delete() error {
	return b.cache.Tx(func(tx *Tx) error {
		return sqlitex.Execute(tx.conn.sqliteConn, "delete from blob where name=?", &sqlitex.ExecOptions{
			Args: []interface{}{b.name},
		})
	})
}

func (p Blob) Size() (l int64, err error) {
	err = p.cache.Tx(func(tx *Tx) (err error) {
		pb, err := tx.OpenPinnedReadOnly(p.name)
		if err != nil {
			return
		}
		defer pb.Close()
		l = pb.Length()
		return
	})
	return
}

func (b Blob) LastUsed() (lastUsed time.Time, err error) {
	err = b.cache.withConn(func(c conn) (err error) {
		lastUsed, err = c.lastUsedByKey(b.name)
		return
	})
	return
}
