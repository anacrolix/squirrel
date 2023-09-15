package squirrel

import (
	g "github.com/anacrolix/generics"
	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

type Tx struct {
	c *Cache
}

type CreateOpts struct {
	Length int64
}

func (tx *Tx) Create(name string, opts CreateOpts) (*PinnedBlob, error) {
	return tx.openBlob(name, g.Some(opts.Length))
}

func (tx *Tx) Open(name string) (pb *PinnedBlob, err error) {
	return tx.openBlob(name, g.None[int64]())
}

func (tx *Tx) openBlob(name string, length g.Option[int64]) (pb *PinnedBlob, err error) {
	blob, rowid, err := tx.c.getBlob(
		name,
		length.Ok,
		length.UnwrapOr(-1),
		true,
		g.None[rowid](),
		true,
	)
	if err != nil {
		return
	}
	pb = &PinnedBlob{
		key:   name,
		rowid: rowid,
		blob:  blob,
		c:     tx.c,
		tx:    tx,
	}
	return
}

func (tx *Tx) Put(name string, b []byte) (err error) {
	// TODO: Reuse blobs cached in the Tx, with reopen?
	blob, _, err := tx.c.getBlob(name, true, int64(len(b)), true, g.None[rowid](), true)
	if err != nil {
		return
	}
	_, err = blobWriteAt(blob, b, 0)
	closeErr := blob.Close()
	if closeErr != nil {
		panic(closeErr)
	}
	return
}

func (tx *Tx) ReadFull(key string, b []byte) (n int, err error) {
	c := tx.c
	blobDataId, err := c.accessBlob(key)
	if err != nil {
		return
	}
	err = sqlitex.Exec(
		c.conn,
		`select data from blob_data where data_id=?`,
		func(stmt *sqlite.Stmt) error {
			n = stmt.ColumnBytes(0, b)
			return nil
		},
		blobDataId,
	)
	return
}

func (tx *Tx) SetTag(key, name string, value any) (err error) {
	return sqlitex.Exec(
		tx.c.conn,
		"insert or replace into tag (blob_name, tag_name, value) values (?, ?, ?)",
		nil,
		key,
		name,
		value,
	)
}

func (tx *Tx) Delete(name string) error {
	return sqlitex.Execute(tx.c.conn, "delete from blob where name=?", &sqlitex.ExecOptions{
		Args: []interface{}{name},
	})
}

// Returns a PinnedBlob. The item must already exist. You must call PinnedBlob.Close when done
// with it.
func (tx *Tx) OpenPinned(name string) (ret *PinnedBlob, err error) {
	return tx.c.openPinned(name, true, tx)
}

// Returns a PinnedBlob. The item must already exist. You must call PinnedBlob.Close when done
// with it.
func (tx *Tx) OpenPinnedReadOnly(name string) (ret *PinnedBlob, err error) {
	return tx.c.openPinned(name, false, tx)
}
