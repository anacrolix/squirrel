package squirrel

import (
	qt "github.com/frankban/quicktest"
	"path/filepath"
	"testing"
)

func TestingNewCache(c *qt.C, opts NewCacheOpts) *Cache {
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

func TestingTempCachePath(c testing.TB) string {
	return filepath.Join(c.TempDir(), "squirrel.db")
}

func TestingDefaultCacheOpts(tb testing.TB) (ret NewCacheOpts) {
	ret.Path = TestingTempCachePath(tb)
	return
}
