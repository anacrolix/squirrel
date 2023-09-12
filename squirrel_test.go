package squirrel_test

import (
	"errors"
	_ "github.com/anacrolix/envpprof"
	"github.com/anacrolix/squirrel"
	qt "github.com/frankban/quicktest"
	sqlite "github.com/go-llsqlite/adapter"
	"io"
	"io/fs"
	"testing"
	"time"
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

func waitSqliteSubsec() {
	// Wait long enough for unixepoch('now', 'subsec') to change in sqlite.
	time.Sleep(2 * time.Millisecond)
}

// Check that we can read while there's a write transaction, and not error due to not being able to
// apply access.
func TestIgnoreBusyUpdatingAccessOnRead(t *testing.T) {
	qtc := qt.New(t)
	cacheOpts := squirrel.TestingDefaultCacheOpts(t)

	c1 := squirrel.TestingNewCache(qtc, cacheOpts)
	defer c1.Close()
	putValue := []byte("mundo")
	err := c1.Put(defaultKey, putValue)
	qtc.Assert(err, qt.IsNil)
	putTime, err := c1.NewBlobRef(defaultKey).LastUsed()
	qtc.Assert(err, qt.IsNil)
	t.Logf("put time: %v", putTime.UnixMilli())

	// There should be no lock on the database.
	c2 := squirrel.TestingNewCache(qtc, cacheOpts)

	waitSqliteSubsec()
	// Start a read transaction.
	writePb, err := c1.OpenPinned(defaultKey)
	qtc.Assert(err, qt.IsNil)
	defer writePb.Close()
	// Upgrade to a write.
	_, err = writePb.WriteAt(defaultValue, 0)
	qtc.Assert(putValue, qt.Not(qt.DeepEquals), defaultValue)
	qtc.Assert(err, qt.IsNil)
	writeTime, err := writePb.LastUsed()
	qtc.Assert(err, qt.IsNil)
	qtc.Assert(writeTime, qt.Not(qt.Equals), putTime)
	qtc.Assert(err, qt.IsNil)
	t.Logf("write time: %v", writeTime.UnixMilli())

	// Check we read the put without error, despite a write transaction being held open by writePb.
	testReadOnlyPinned(qtc, c2, defaultKey, putValue, putTime, false)
	writePb.Close()
	// Now check that we read the new written value, and our read updates access.
	testReadOnlyPinned(qtc, c2, defaultKey, defaultValue, writeTime, true)
}

func testReadOnlyPinned(
	qtc *qt.C,
	cache *squirrel.Cache,
	key string,
	value []byte,
	lastUsed time.Time,
	expectAccessUpdate bool,
) {
	r2, err := cache.OpenPinnedReadOnly(key)
	defer r2.Close()
	beforeRead, err := r2.LastUsed()
	qtc.Check(beforeRead.UnixMilli(), qt.Equals, lastUsed.UnixMilli())
	waitSqliteSubsec()
	b2, err := io.ReadAll(io.NewSectionReader(r2, 0, r2.Length()))
	qtc.Assert(err, qt.IsNil)
	qtc.Check(b2, qt.DeepEquals, value)
	afterRead, err := r2.LastUsed()
	qtc.Assert(err, qt.IsNil)
	if expectAccessUpdate {
		qtc.Check(afterRead, qt.Not(qt.Equals), beforeRead)
	} else {
		qtc.Check(afterRead, qt.Equals, beforeRead)
	}
}

// Check that we can read while there's a write transaction, and not error due to not being able to
// apply access.
func TestNewCacheWaitsForWrite(t *testing.T) {
	qtc := qt.New(t)
	cacheOpts := squirrel.TestingDefaultCacheOpts(t)

	c1 := squirrel.TestingNewCache(qtc, cacheOpts)
	defer c1.Close()
	putValue := []byte("mundo")
	err := c1.Put(defaultKey, putValue)
	qtc.Assert(err, qt.IsNil)

	// Start a read transaction.
	writePb, err := c1.OpenPinned(defaultKey)
	qtc.Assert(err, qt.IsNil)
	defer writePb.Close()
	// Upgrade to a write.
	_, err = writePb.WriteAt(defaultValue, 0)
	qtc.Assert(putValue, qt.Not(qt.DeepEquals), defaultValue)
	qtc.Assert(err, qt.IsNil)

	// Wait long enough that the following NewCache should be blocked trying to init the schema.
	time.AfterFunc(time.Second, func() {
		writePb.Close()
	})
	c2 := squirrel.TestingNewCache(qtc, cacheOpts)
	c2.Close()
}
