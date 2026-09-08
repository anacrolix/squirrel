package squirrel

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-quicktest/qt"
)

func TestingNewCache(tb testing.TB, opts NewCacheOpts) *Cache {
	if opts.SetJournalMode == "wal" && (opts.Memory || opts.Path == "") {
		tb.Skip("can't use WAL with anonymous or memory database")
	}
	if opts.Memory && opts.SetLockingMode != "exclusive" {
		tb.Skip("in-memory databases are always exclusive")
	}
	if opts.Path == "" && opts.SetLockingMode == "normal" {
		tb.Skip("anonymous databases are always exclusive")
	}
	cache, err := NewCache(opts)
	qt.Assert(tb, qt.IsNil(err))
	tb.Cleanup(func() {
		err := cache.Close()
		qt.Check(tb, qt.IsNil(err))
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
	//ret.SetJournalMode = "wal"
	return
}
