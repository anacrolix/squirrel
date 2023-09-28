package squirrel

import (
	_ "embed"
	"errors"
	"fmt"
	g "github.com/anacrolix/generics"
	"github.com/anacrolix/log"
	"net/url"
	"time"

	"github.com/ajwerner/btree"

	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
)

type sqliteConn = *sqlite.Conn

type connStruct struct {
	sqliteConn  sqliteConn
	blobs       btree.Map[valueKey, *sqlite.Blob]
	maxBlobSize maxBlobSizeType
	logger      log.Logger
}

func (c conn) Close() error {
	return c.sqliteConn.Close()
}

type conn = *connStruct

func initSqliteConn(conn sqliteConn, opts InitConnOpts, pageSize int) (err error) {
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
	if opts.LengthLimit.Ok {
		conn.Limit(sqlite.LimitLength, opts.LengthLimit.Value)
	}
	// Setting this to zero seems to have a decent performance impact when trimming is enabled.
	if opts.JournalSizeLimit.Ok {
		err = setAndVerifyPragma(conn, "journal_size_limit", opts.JournalSizeLimit.Value)
		if err != nil {
			return
		}
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
	if err != nil {
		return
	}
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

// This is a wrapper to trim and tidy the sql from the source if needed. It's also easier to format
// a query wrapped in a function to look good.
func sqlQuery(query string) string {
	return query
}

func (conn conn) iterBlobs(
	valueId rowid,
	iter func(offset int64, blob *sqlite.Blob) (more bool, err error),
	write bool,
	startOffset int64,
) (err error) {
	it := conn.blobs.Iterator()
	// Seek to the blob after the one we want, because we need the blob that will contain our
	// intended offset.
	it.SeekGE(valueKey{
		keyId:  valueId,
		offset: startOffset + 1,
	})
	if it.Valid() {
		it.Prev()
	} else {
		// There's a test that shows this is necessary. You can't Prev on an invalid iterator.
		it.Last()
	}
	// Technically don't need more here yet. From here we reuse blobs, then get new ones by querying
	// the database. Once the statement begins, we check into the cache before creating new blobs,
	// but keep the statement running.
	more := true
	for it.Valid() && it.Cur().keyId == valueId {
		blobEnd := it.Cur().offset + it.Value().Size()
		if blobEnd > startOffset {
			more, err = iter(it.Cur().offset, it.Value())
			if err != nil || !more {
				return
			}
			// Skip to the first unknown offset if we have to query for it.
			startOffset = blobEnd
		}
		it.Next()
	}
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
			//log.Println(offset, blobId)
			key := valueKey{
				keyId:  valueId,
				offset: offset,
			}
			blob, ok := conn.blobs.Get(key)
			if !ok {
				blob, err = conn.openBlob(blobId, write)
				if err == nil {
					_, oldBlob, replaced := conn.blobs.Upsert(key, blob)
					if replaced {
						// If we close this blob before it leaks, we can clean up tests nicely
						// despite panicking.
						oldBlob.Close()
						panic(key)
					}
				}
				if err != nil {
					err = fmt.Errorf("error opening blob id %v for offset %v: %w", blobId, offset, err)
					return
				}
			}
			more, err = iter(offset, blob)
			return
		},
		valueId,
		startOffset,
	)
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
			return
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

func (conn conn) accessedKey(keyId rowid, ignoreBusy bool) (ignored bool, err error) {
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
	if ignoreBusy && sqlite.IsPrimaryResultCodeErr(err, sqlite.ResultCodeBusy) {
		ignored = true
		err = nil
	}
	return
}

func (conn conn) closeBlobs() {
	it := conn.blobs.Iterator()
	it.First()
	for it.Valid() {
		it.Value().Close()
		it.Next()
	}
	conn.blobs.Reset()
}

func (conn conn) forgetBlobsForKeyId(keyId rowid) (err error) {
	it := conn.blobs.Iterator()
	for {
		it.SeekGE(valueKey{
			keyId:  keyId,
			offset: 0,
		})
		if !it.Valid() || it.Cur().keyId != keyId {
			break
		}
		err = errors.Join(err, it.Value().Close())
		conn.blobs.Delete(it.Cur())
	}
	return
}

const logTrimmedKeys = true

func (conn conn) trimToCapacity(eachKey func(keyId rowid)) (err error) {
	capacity, err := conn.getCapacity()
	if err != nil {
		return
	}
	if !capacity.Ok {
		return
	}
	for {
		var bytesUsed int64
		bytesUsed, err = conn.bytesUsed()
		if err != nil {
			return
		}
		if bytesUsed <= capacity.Value {
			return
		}
		var (
			key         string
			lastUsed    time.Time
			accessCount int64
			createTime  time.Time
			length      int64
			keyId       int64
		)
		ok, err := conn.sqliteQueryRow(
			sqlQuery(`
				delete from keys
				where key_id=(select key_id from keys order by last_used, access_count, create_time limit 1)
				returning key, last_used, access_count, create_time, length, key_id
			`),
			func(stmt *sqlite.Stmt) error {
				if logTrimmedKeys {
					key = stmt.ColumnText(0)
					lastUsed = timeFromStmtColumn(stmt, 1)
					accessCount = stmt.ColumnInt64(2)
					createTime = timeFromStmtColumn(stmt, 3)
					length = stmt.ColumnInt64(4)
				}
				keyId = stmt.ColumnInt64(5)
				return nil
			},
		)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("couldn't find keys to delete")
		}
		if eachKey != nil {
			eachKey(keyId)
		}
		if logTrimmedKeys {
			conn.logger.Levelf(
				log.Debug,
				"trimmed key %q (size %v, last used %v ago, access count %v, created %v ago)",
				key,
				length,
				time.Since(lastUsed).Truncate(time.Second),
				accessCount,
				time.Since(createTime).Truncate(time.Second),
			)
		}
	}
}

func (conn conn) bytesUsed() (ret int64, err error) {
	pages, err := conn.execPragmaReturningInt64("page_count")
	if err != nil {
		return
	}
	pageSize, err := conn.execPragmaReturningInt64("page_size")
	if err != nil {
		return
	}
	freelistCount, err := conn.execPragmaReturningInt64("freelist_count")
	if err != nil {
		return
	}
	ret = (pages - freelistCount) * pageSize
	return
}

func (conn conn) execPragmaReturningInt64(pragma string) (ret int64, err error) {
	err = conn.sqliteQueryMustOneRow(fmt.Sprintf("pragma %v", pragma), func(stmt *sqlite.Stmt) error {
		ret = stmt.ColumnInt64(0)
		return nil
	})
	return
}

func (conn conn) getCapacity() (capacity g.Option[int64], err error) {
	err = conn.sqliteQueryMaxOneRow(
		"select value from setting where name='capacity'",
		func(stmt *sqlite.Stmt) error {
			capacity.Set(stmt.ColumnInt64(0))
			return nil
		},
	)
	return
}
