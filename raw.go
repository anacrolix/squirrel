package squirrel

import (
	"github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

func createBlob(c conn, name string, length int64, clobber bool) (rowid int64, err error) {
	// end, err := sqlitex.ImmediateTransaction(c)
	// if err != nil {
	// 	err = fmt.Errorf("beginning transaction: %w", err)
	// 	return
	// }
	// defer end(&err)
	// sqlitex.Exec(c, "begin", nil)
	// defer func() {
	// 	if err != nil {
	// 		sqlitex.Exec(c, "rollback", nil)
	// 	} else {
	// 		sqlitex.Exec(c, "end", nil)
	// 	}
	// }()
	// defer sqlitex.Transaction(c)(&err)
	if clobber {
		var dataId setOnce[int64]
		err = sqlitex.Exec(c, "select data_id from blob where name=?", func(stmt *sqlite.Stmt) error {
			dataId.Set(stmt.ColumnInt64(0))
			return nil
		}, name)
		if err != nil {
			return
		}
		if dataId.Ok() {
			// log.Printf("clobbering %q to length %v", name, length)
			err = sqlitex.Execute(c, `
				update blob_data set data=zeroblob(?) where data_id=?`,
				&sqlitex.ExecOptions{
					Args: []interface{}{length, dataId.Value()},
				})
			if err != nil {
				return
			}
			if c.Changes() != 1 {
				panic("expected single replace")
			}
			rowid = dataId.Value()
			return
		}
	}
	err = sqlitex.Execute(c, "insert into blob_data(data) values (zeroblob(?))", &sqlitex.ExecOptions{
		Args: []interface{}{length},
	})
	if err != nil {
		return
	}
	rowid = c.LastInsertRowID()
	if rowid == 0 {
		panic(rowid)
	}
	err = sqlitex.Execute(c, "insert or replace into blob(name, data_id) values (?, ?)", &sqlitex.ExecOptions{
		Args: []interface{}{name, rowid},
	})
	return
}

func rowidForBlob(c conn, name string) (rowid int64, length int64, ok bool, err error) {
	err = sqlitex.Exec(c, "select data_id, length(cast(data as blob)) from blob join blob_data using (data_id) where name=?", func(stmt *sqlite.Stmt) error {
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
