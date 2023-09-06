package squirrel

import (
	_ "embed"
	"errors"
	"fmt"
	"github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
	"net/url"
)

type conn = *sqlite.Conn

type ErrUnexpectedJournalMode struct {
	JournalMode string
}

func (me ErrUnexpectedJournalMode) Error() string {
	return fmt.Sprintf("unexpected journal mode: %q", me.JournalMode)
}

func setSynchronous(conn conn, syncInt int) (err error) {
	err = sqlitex.ExecTransient(conn, fmt.Sprintf(`pragma synchronous=%v`, syncInt), nil)
	if err != nil {
		return err
	}
	var (
		actual   int
		actualOk bool
	)
	err = sqlitex.ExecTransient(conn, `pragma synchronous`, func(stmt *sqlite.Stmt) error {
		actual = stmt.ColumnInt(0)
		actualOk = true
		return nil
	})
	if err != nil {
		return
	}
	if !actualOk {
		return errors.New("synchronous setting query didn't return anything")
	}
	if actual != syncInt {
		return fmt.Errorf("set synchronous %q, got %q", syncInt, actual)
	}
	return nil
}

func setAndVerifyPragma(conn conn, name string, value any) (err error) {
	valueStr := fmt.Sprint(value)
	var once setOnce[string]
	setPragmaQuery := fmt.Sprintf("pragma %s=%s", name, valueStr)
	err = sqlitex.ExecTransient(
		conn,
		setPragmaQuery,
		func(stmt *sqlite.Stmt) error {
			once.Set(stmt.ColumnText(0))
			return nil
		},
	)
	if err != nil {
		return
	}
	if !once.Ok() {
		err = sqlitex.ExecTransient(
			conn,
			fmt.Sprintf("pragma %s", name),
			func(stmt *sqlite.Stmt) error {
				once.Set(stmt.ColumnText(0))
				return nil
			},
		)
		if err != nil {
			return
		}
	}
	if once.Value() != valueStr {
		err = fmt.Errorf("%q returned %q", setPragmaQuery, once.Value())
	}
	return

}

func execTransientReturningText(conn conn, query string) (s string, err error) {
	err = sqlitex.ExecTransient(conn, query, func(stmt *sqlite.Stmt) error {
		s = stmt.ColumnText(0)
		return nil
	})
	return
}

func initConn(conn conn, opts InitConnOpts, pageSize int) (err error) {
	err = sqlitex.ExecTransient(conn, "pragma foreign_keys=on", nil)
	if err != nil {
		return err
	}
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
		journalMode, err := execTransientReturningText(conn, fmt.Sprintf(`pragma journal_mode=%s`, opts.SetJournalMode))
		if err != nil {
			return err
		}
		if journalMode != opts.SetJournalMode {
			return ErrUnexpectedJournalMode{journalMode}
		}
	}
	if opts.SetLockingMode != "" {
		mode, err := execTransientReturningText(conn, "pragma locking_mode="+opts.SetLockingMode)
		if err != nil {
			return err
		}
		if mode != opts.SetLockingMode {
			return fmt.Errorf("error setting locking_mode, got %q", mode)
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
	err = setAndVerifyPragma(conn, "cache_size", fmt.Sprint(-32<<20))
	return
}

func setPageSize(conn conn, pageSize int) error {
	if pageSize == 0 {
		return nil
	}
	var retSize int64
	err := sqlitex.ExecTransient(conn, fmt.Sprintf(`pragma page_size=%d`, pageSize), nil)
	if err != nil {
		return err
	}
	err = sqlitex.ExecTransient(conn, "pragma page_size", func(stmt *sqlite.Stmt) error {
		retSize = stmt.ColumnInt64(0)
		return nil
	})
	if err != nil {
		return err
	}
	if retSize != int64(pageSize) {
		return fmt.Errorf("requested page size %v but got %v", pageSize, retSize)
	}
	return nil
}

var (
	//go:embed init.sql
	initScript string
	//go:embed init-triggers.sql
	initTriggers string
)

func InitSchema(conn conn, pageSize int, triggers bool) error {
	err := setPageSize(conn, pageSize)
	if err != nil {
		return fmt.Errorf("setting page size: %w", err)
	}
	err = sqlitex.ExecScript(conn, initScript)
	if err != nil {
		return err
	}
	if triggers {
		err := sqlitex.ExecScript(conn, initTriggers)
		if err != nil {
			return fmt.Errorf("initing triggers: %w", err)
		}
	}
	return nil
}

// Remove any capacity limits.
func unlimitCapacity(conn conn) error {
	return sqlitex.Exec(conn, "delete from setting where name='capacity'", nil)
}

// Set the capacity limit to exactly this value.
func setCapacity(conn conn, cap int64) error {
	return sqlitex.Exec(conn, "insert into setting values ('capacity', ?)", nil, cap)
}

func newOpenUri(opts NewConnOpts) string {
	path := url.PathEscape(opts.Path)
	if opts.Memory {
		path = ":memory:"
	}
	values := make(url.Values)
	if opts.NoConcurrentBlobReads || opts.Memory {
		values.Add("cache", "shared")
	}
	// This still seems to use temporary databases as expected when there's just ?, so no need to
	// special case empty paths and empty queries.
	return fmt.Sprintf("file:%s?%s", path, values.Encode())
}

func initDatabase(conn conn, opts InitDbOpts) (err error) {
	if !opts.DontInitSchema {
		err = InitSchema(conn, opts.PageSize, !opts.NoTriggers)
		if err != nil {
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

func newConn(opts NewConnOpts) (conn, error) {
	uri := newOpenUri(opts)
	//log.Printf("opening sqlite conn with uri %q", uri)
	return sqlite.OpenConn(uri, openConnFlags)
}
