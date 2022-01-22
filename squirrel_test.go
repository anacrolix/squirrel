package squirrel

import (
	"errors"
	"io/fs"
	"testing"

	qt "github.com/frankban/quicktest"
)

func errorIs(target error) func(error) bool {
	return func(err error) bool {
		return errors.Is(err, target)
	}
}

func TestBlobWriteOutOfBounds(t *testing.T) {
	c := qt.New(t)
	cache, err := NewCache(NewCacheOpts{})
	c.Assert(err, qt.IsNil)
	defer cache.Close()
	_, err = cache.Open("greeting")
	c.Check(err, qt.Satisfies, errorIs(fs.ErrNotExist))
	b := cache.BlobWithLength("greeting", 6)
	n, err := b.WriteAt([]byte("hello "), 0)
	c.Assert(err, qt.IsNil)
	c.Check(n, qt.Equals, 6)
	n, err = b.WriteAt([]byte("world\n"), 6)
	c.Check(n, qt.Equals, 0)
	c.Check(err, qt.ErrorMatches, `.*bound.*\bSQLITE_ERROR`)
	c.Check(cache.Close(), qt.IsNil)
}
