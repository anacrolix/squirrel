package squirrel

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
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
	if cleanupDatabases {
		// Put the database in the test temp dir, so it gets removed automatically.
		return filepath.Join(c.TempDir(), "squirrel.db")
	}
	// Create a temporary file in the OS temp dir, so we can inspect it after the tests.
	f, err := os.CreateTemp("", "squirrel.db")
	if err != nil {
		c.Fatalf("creating temp cache path: %v", err)
	}
	path := f.Name()
	c.Logf("cache path: %v", path)
	f.Close()
	return path
}

// Whether to remove databases after tests run, or leave them behind and log where they are for
// inspection.
const cleanupDatabases = true

func TestingDefaultCacheOpts(tb testing.TB) (ret NewCacheOpts) {
	ret.Path = TestingTempCachePath(tb)
	return
}
