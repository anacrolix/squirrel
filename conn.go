package squirrel

import (
	_ "embed"
	"fmt"
	"github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
	"net/url"
)

type conn = *sqlite.Conn

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

func setPageSize(conn conn, pageSize int) error {
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

func newConn(opts NewConnOpts) (conn, error) {
	uri := newOpenUri(opts)
	//log.Printf("opening sqlite conn with uri %q", uri)
	return sqlite.OpenConn(uri, openConnFlags)
}
