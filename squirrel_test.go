package squirrel_test

import (
	"errors"
	_ "github.com/anacrolix/envpprof"
	"github.com/anacrolix/squirrel"
	qt "github.com/frankban/quicktest"
	sqlite "github.com/go-llsqlite/adapter"
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
	cache := squirrel.TestingNewCache(c, squirrel.NewCacheOpts{})
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

func TestTagDeletedWithBlob(t *testing.T) {
	c := qt.New(t)
	opts := squirrel.NewCacheOpts{}
	opts.Capacity = 43
	cache := squirrel.TestingNewCache(c, squirrel.NewCacheOpts{})
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
