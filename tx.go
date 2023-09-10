package squirrel

import (
	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

type Tx struct {
	c *Cache
}

func (tx *Tx) Put(name string, b []byte) (err error) {
	return Blob{
		name:   name,
		length: int64(len(b)),
		cache:  tx.c,
	}.doWithBlob(func(blob *sqlite.Blob) error {
		_, err := blobWriteAt(blob, b, 0)
		return err
	}, true, true)
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
