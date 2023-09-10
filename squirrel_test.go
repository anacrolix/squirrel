package squirrel

import (
	"errors"
	_ "github.com/anacrolix/envpprof"
	"github.com/anacrolix/log"
	qt "github.com/frankban/quicktest"
	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
	"golang.org/x/sync/errgroup"
	"io"
	"io/fs"
	"testing"
)

func errorIs(target error) func(error) bool {
	return func(err error) bool {
		return errors.Is(err, target)
	}
}

func TestBlobWriteOutOfBounds(t *testing.T) {
	c := qt.New(t)
	cache := newCache(c, NewCacheOpts{})
	_, err := cache.OpenPinned("greeting")
	c.Check(err, qt.Satisfies, errorIs(fs.ErrNotExist))
	b := cache.BlobWithLength("greeting", 6)
	n, err := b.WriteAt([]byte("hello "), 0)
	c.Assert(err, qt.IsNil)
	c.Check(n, qt.Equals, 6)
	n, err = b.WriteAt([]byte("world\n"), 6)
	c.Check(n, qt.Equals, 0)
	c.Check(sqlite.ErrCode(err), qt.Equals, sqlite.ResultCodeGenericError)
	c.Check(cache.Close(), qt.IsNil)
}

func newCache(c *qt.C, opts NewCacheOpts) *Cache {
	if opts.SetJournalMode == "wal" && (opts.Memory || opts.Path == "") {
		c.Skip("can't use WAL with anonymous or memory database")
	}
	if opts.Memory && opts.SetLockingMode != "exclusive" {
		c.Skip("in-memory databases are always exclusive")
	}
	if opts.Path == "" && opts.SetLockingMode == "normal" {
		c.Skip("anonymous databases are always exclusive")
	}
	cache, err := NewCache(opts)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		err := cache.Close()
		c.Check(err, qt.IsNil)
	})
	return cache
}

func TestTagDeletedWithBlob(t *testing.T) {
	c := qt.New(t)
	opts := NewCacheOpts{}
	opts.Capacity = 43
	cache := newCache(c, NewCacheOpts{})
	b := cache.OpenWithLength("hello", 42)
	b.SetTag("gender", "yes")
	c.Assert(b.GetTag("gender", func(stmt *sqlite.Stmt) {
		c.Check(stmt.ColumnText(0), qt.Equals, "yes")
	}), qt.IsNil)
	b.Delete()
	var tagOk bool
	b.GetTag("gender", func(stmt *sqlite.Stmt) {
		tagOk = true
	})
	c.Check(tagOk, qt.IsFalse)
}

func TestConcurrentCreateBlob(t *testing.T) {
	c := qt.New(t)
	opts := defaultCacheOpts(c)
	opts.Capacity = -1
	logger := log.Default.WithNames("test")
	t.Logf("shared path: %q", opts.Path)
	opts.SetJournalMode = "wal"
	var eg errgroup.Group
	doPut := func(value string) func() error {
		cache := newCache(c, opts)
		logger.Levelf(log.Debug, "opened cache for %q", value)
		return func() (err error) {
			logger.Levelf(log.Debug, "putting %q", value)
			err = cache.Put("greeting", []byte(value))
			logger.Levelf(log.Debug, "put %q: %v", value, err)
			return
		}
	}
	allValues := []string{
		"hello",
		"world!",
		"wake and bake",
		`31337 45 |=\/(|<`,
	}
	var jobs []func() error
	for _, v := range allValues {
		jobs = append(jobs, doPut(v))
	}
	for _, j := range jobs {
		eg.Go(j)
	}
	c.Assert(eg.Wait(), qt.IsNil)
	cache := newCache(c, opts)
	pb, err := cache.OpenPinned("greeting")
	c.Assert(err, qt.IsNil)
	b, err := io.ReadAll(io.NewSectionReader(pb, 0, pb.Length()))
	c.Check(pb.Close(), qt.IsNil)
	c.Check(err, qt.IsNil)
	c.Check(allValues, qt.Contains, string(b))
	conn, err := newConn(opts.NewConnOpts)
	c.Assert(err, qt.IsNil)
	defer conn.Close()
	var count setOnce[int64]
	c.Assert(sqlitex.Exec(conn, "select count(*) from blob_data", func(stmt *sqlite.Stmt) error {
		count.Set(stmt.ColumnInt64(0))
		return nil
	}), qt.IsNil)
	c.Check(count.Value(), qt.Equals, int64(1))
}
