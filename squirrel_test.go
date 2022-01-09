package squirrel

import (
	"errors"
	"io/fs"
	"testing"

	_ "github.com/anacrolix/envpprof"
	qt "github.com/frankban/quicktest"
	"zombiezen.com/go/sqlite"
)

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
