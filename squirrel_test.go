package squirrel

import (
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/anacrolix/envpprof"
	qt "github.com/frankban/quicktest"
	"golang.org/x/sync/errgroup"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func init() {
	log.SetFlags(log.Flags() | log.Lshortfile)
}

func errorIs(target error) func(error) bool {
	return func(err error) bool {
		return errors.Is(err, target)
	}
}

func TestBlobWriteOutOfBounds(t *testing.T) {
	c := qt.New(t)
	cache := newCache(c, NewCacheOpts{})
	_, err := cache.Open("greeting")
	c.Check(err, qt.Satisfies, errorIs(fs.ErrNotExist))
	b := cache.BlobWithLength("greeting", 6)
	n, err := b.WriteAt([]byte("hello "), 0)
	c.Assert(err, qt.IsNil)
	c.Check(n, qt.Equals, 6)
	n, err = b.WriteAt([]byte("world\n"), 6)
	c.Check(n, qt.Equals, 0)
	c.Check(sqlite.ErrCode(err), qt.Equals, sqlite.ResultError)
	c.Check(cache.Close(), qt.IsNil)
}

func newCache(c *qt.C, opts NewCacheOpts) *Cache {
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
	opts := NewCacheOpts{}
	tempDir, _ := os.MkdirTemp("", "")
	opts.Path = filepath.Join(tempDir, "db")
	opts.Capacity = -1
	// opts.NoCacheBlobs = true
	// opts.SetSynchronous = 1
	// opts.PageSize = 4096
	t.Logf("shared path: %q", opts.Path)
	opts.SetJournalMode = "wal"
	var eg errgroup.Group
	doPut := func(value string) func() error {
		cache := newCache(c, opts)
		t.Logf("opened cache for %q", value)
		return func() error {
			log.Printf("putting %q", value)
			return cache.Put("greeting", []byte(value))
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
	c.Check(eg.Wait(), qt.IsNil)
	cache := newCache(c, opts)
	pb, err := cache.Open("greeting")
	c.Assert(err, qt.IsNil)
	b, err := io.ReadAll(pb)
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
