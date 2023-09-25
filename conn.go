package squirrel

import (
	_ "embed"
	"fmt"
	g "github.com/anacrolix/generics"
	"net/url"

	"github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

type sqliteConn = *sqlite.Conn

type connStruct struct {
	sqliteConn  sqliteConn
	blobs       map[rowid]*sqlite.Blob
	maxBlobSize maxBlobSizeType
}

func (c conn) Close() error {
	return c.sqliteConn.Close()
}

type conn connStruct

func initConn(conn sqliteConn, opts InitConnOpts, pageSize int) (err error) {
	err = setSynchronous(conn, opts.SetSynchronous)
	if err != nil {
		return
	}
	// Recursive triggers are required because we need to trim the blob_meta size after trimming to
	// capacity. Hopefully we don't hit the recursion limit, and if we do, there's an error thrown.
	err = sqlitex.ExecTransient(conn, "pragma recursive_triggers=on", nil)
	if err != nil {
		return err
	}
	// For some reason it's faster to set page size after synchronous. We need to set it before
	// setting journal mode in case it's WAL.
	err = setPageSize(conn, pageSize)
	if err != nil {
		err = fmt.Errorf("setting page size: %w", err)
		return
	}
	if opts.SetJournalMode != "" {
		journalMode, err := execTransientReturningText(
			conn,
			fmt.Sprintf(`pragma journal_mode=%s`, opts.SetJournalMode),
		)
		if err != nil {
			return err
		}
		// Pragma journal_mode always returns the journal mode.
		if journalMode.Unwrap() != opts.SetJournalMode {
			return ErrUnexpectedJournalMode{journalMode.Unwrap()}
		}
	}
	if opts.SetLockingMode != "" {
		err := setAndVerifyPragma(conn, "locking_mode", opts.SetLockingMode)
		if err != nil {
			return err
		}
	}
	if !opts.MmapSizeOk {
		// Set the default. Currently it seems the library picks reasonable defaults, especially for
		// wal.
		opts.MmapSize = -1
		// opts.MmapSize = 1 << 24 // 8 MiB
	}
	if opts.MmapSize >= 0 {
		err = setAndVerifyPragma(conn, "mmap_size", opts.MmapSize)
		if err != nil {
			return err
		}
	}
	if opts.CacheSize.Ok {
		err = setAndVerifyPragma(conn, "cache_size", opts.CacheSize.Value)
	}
	return
}

func setPageSize(conn sqliteConn, pageSize int) error {
	if pageSize == 0 {
		return nil
	}
	return setAndVerifyPragma(conn, "page_size", pageSize)
}

var (
	//go:embed init.sql
	initScript string
	//go:embed init-triggers.sql
	initTriggers string
)

func InitSchema(conn sqliteConn, pageSize int, triggers bool) (err error) {
	err = setPageSize(conn, pageSize)
	if err != nil {
		return fmt.Errorf("setting page size: %w", err)
	}
	// By starting immediately into a write, we can block rather than get SQLITE_BUSY for trying to
	// upgrade from a read later.
	return sqlitex.WithTransactionRollbackOnError(conn, `immediate`, func() (err error) {
		err = sqlitex.ExecScript(conn, initScript)
		if err != nil {
			return
		}
		if triggers {
			err = sqlitex.ExecScript(conn, initTriggers)
			if err != nil {
				err = fmt.Errorf("initing triggers: %w", err)
				return
			}
		}
		return
	})
}

// Remove any capacity limits.
func unlimitCapacity(conn sqliteConn) error {
	return sqlitex.Exec(conn, "delete from setting where name='capacity'", nil)
}

// Set the capacity limit to exactly this value.
func setCapacity(conn sqliteConn, cap int64) error {
	return sqlitex.Exec(conn, "insert into setting values ('capacity', ?)", nil, cap)
}

func newOpenUri(opts NewConnOpts) string {
	path := url.PathEscape(opts.Path)
	if opts.Memory {
		path = ":memory:"
	}
	values := make(url.Values)
	if opts.Memory {
		values.Add("cache", "shared")
	}
	// This still seems to use temporary databases as expected when there's just ?, so no need to
	// special case empty paths and empty queries.
	return fmt.Sprintf("file:%s?%s", path, values.Encode())
}

func initDatabase(conn sqliteConn, opts InitDbOpts) (err error) {
	if opts.SetAutoVacuum.Ok {
		// This needs to occur before setting journal mode to WAL.
		err = setAndMaybeVerifyPragma(
			conn,
			"auto_vacuum",
			opts.SetAutoVacuum.Value,
			opts.RequireAutoVacuum,
		)
		if err != nil {
			return err
		}
	} else if opts.RequireAutoVacuum.Ok {
		autoVacuumValue, err := execTransientReturningText(conn, "pragma auto_vacuum")
		if err != nil {
			return err
		}
		if autoVacuumValue.Unwrap() != opts.RequireAutoVacuum.Value {
			err = fmt.Errorf(
				"auto_vacuum is %q not %q",
				autoVacuumValue.Value,
				opts.RequireAutoVacuum.Value,
			)
			return err
		}
	}
	if !opts.DontInitSchema {
		err = InitSchema(conn, opts.PageSize, !opts.NoTriggers)
		if err != nil {
			err = fmt.Errorf("initing schema: %w", err)
			return
		}
	}
	if opts.Capacity < 0 {
		err = unlimitCapacity(conn)
	} else if opts.Capacity > 0 {
		err = setCapacity(conn, opts.Capacity)
	}
	return
}

// Go fmt, why you so shit? We specifically don't open with WAL.
const openConnFlags = 0 |
	sqlite.OpenReadWrite |
	sqlite.OpenCreate |
	sqlite.OpenURI |
	sqlite.OpenNoMutex

func newSqliteConn(opts NewConnOpts) (sqliteConn, error) {
	uri := newOpenUri(opts)
	//log.Printf("opening sqlite conn with uri %q", uri)
	return sqlite.OpenConn(uri, openConnFlags)
}

func (conn conn) getValueIdForKey(key string) (ret rowid, err error) {
	keyCols, err := conn.openKey(key)
	ret = keyCols.id
	return
}

func (conn conn) sqliteQuery(query string, result func(stmt *sqlite.Stmt) error, args ...any) error {
	return sqlitex.Exec(conn.sqliteConn, query, result, args...)
}

// Wraps sqliteQueryRow, without returning the ok bool.
func (conn conn) sqliteQueryMaxOneRow(query string, result func(stmt *sqlite.Stmt) error, args ...any) (err error) {
	_, err = conn.sqliteQueryRow(query, result, args...)
	return
}

// Executes sqlite query, panicking if there's more than one row. Returns ok if a row matched.
func (conn conn) sqliteQueryRow(
	query string,
	result func(stmt *sqlite.Stmt) error,
	args ...any,
) (ok bool, err error) {
	err = conn.sqliteQuery(
		query,
		func(stmt *sqlite.Stmt) error {
			if ok {
				panic("got more than one row")
			}
			ok = true
			return result(stmt)
		},
		args...,
	)
	return
}

func (conn conn) sqliteQueryMustOneRow(
	query string,
	result func(stmt *sqlite.Stmt) error,
	args ...any,
) (err error) {
	hadResult := false
	err = conn.sqliteQuery(
		query,
		func(stmt *sqlite.Stmt) error {
			if hadResult {
				panic("got more than one row")
			}
			hadResult = true
			return result(stmt)
		},
		args...,
	)
	if !hadResult {
		panic("got no results")
	}
	return
}

func (conn conn) getValueLength(key string) (length int64, err error) {
	err = conn.sqliteQuery(
		`select length from keys where key=?`,
		func(stmt *sqlite.Stmt) error {
			length = stmt.ColumnInt64(0)
			return nil
		},
		key,
	)
	return
}

func (conn conn) openBlob(blobId rowid, write bool) (*sqlite.Blob, error) {
	return openSqliteBlob(conn.sqliteConn, blobId, write)
}

func sqlQuery(query string) string {
	return query
}

// TODO: Add optimization to skip to first blob that includes an offset
func (conn conn) iterBlobs(
	valueId rowid,
	iter func(offset int64, blob *sqlite.Blob) (more bool, err error),
	write bool,
	startOffset int64,
) (err error) {
	more := true
	var blob g.Option[*sqlite.Blob]
	err = conn.sqliteQuery(
		sqlQuery(`
			select offset, blob_id 
			from "values" join blobs using (blob_id) 
			where value_id=? and offset+length(blob) > ?
			order by offset`,
		),
		func(stmt *sqlite.Stmt) (err error) {
			if !more {
				return
			}
			offset := stmt.ColumnInt64(0)
			blobId := stmt.ColumnInt64(1)
			if !blob.Ok {
				blob.Value, err = conn.openBlob(blobId, write)
				blob.Ok = err == nil
			} else {
				err = blob.Value.Reopen(blobId)
			}
			if err != nil {
				return
			}
			more, err = iter(offset, blob.Unwrap())
			return
		},
		valueId,
		startOffset,
	)
	if blob.Ok {
		blob.Value.Close()
	}
	return
}

func (conn conn) openKey(key string) (ret keyCols, err error) {
	ok, err := conn.sqliteQueryRow(
		`select key_id, length from keys where key=?`,
		func(stmt *sqlite.Stmt) error {
			ret.id = stmt.ColumnInt64(0)
			ret.length = stmt.ColumnInt64(1)
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

func (conn conn) createKey(key string, create CreateOpts) (keyId rowid, err error) {
	cols, err := conn.openKey(key)
	if err != ErrNotFound {
		keyId = cols.id
		return
	}
	err = conn.sqliteQueryMustOneRow(
		`insert into keys (key, length) values (?, ?) returning key_id`,
		func(stmt *sqlite.Stmt) error {
			keyId = stmt.ColumnInt64(0)
			return nil
		},
		key,
		create.Length,
	)
	if err != nil {
		return
	}
	for off := int64(0); off < create.Length; off += conn.maxBlobSize {
		blobSize := create.Length - off
		if blobSize > conn.maxBlobSize {
			blobSize = conn.maxBlobSize
		}
		err = conn.sqliteExec(
			`insert into blobs (blob) values (zeroblob(?))`,
			blobSize,
		)
		if err != nil {
			panic(err)
		}
		blobId := conn.sqliteConn.LastInsertRowID()
		err = conn.sqliteExec(
			`insert into "values" (value_id, offset, blob_id) values (?, ?, ?)`,
			keyId, off, blobId,
		)
		if err != nil {
			panic(err)
		}
	}
	return
}

const defaultMaxBlobSize int64 = 1 << 20

func (conn conn) sqliteExec(query string, args ...any) error {
	return conn.sqliteQuery(query, nil, args...)
}

func (conn conn) accessedKey(keyId rowid, ignoreBusy bool) (err error) {
	err = conn.sqliteExec(
		sqlQuery(`
			update keys
			set 
				last_used=cast(unixepoch('subsec')*1e3 as integer),
				access_count=access_count+1
			where key_id=?`,
		),
		keyId,
	)
	if ignoreBusy && sqlite.IsResultCode(err, sqlite.ResultCodeBusy) {
		err = nil
	}
	return
}
