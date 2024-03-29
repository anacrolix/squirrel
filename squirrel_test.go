package squirrel_test

import (
	"context"
	squirrelTesting "github.com/anacrolix/squirrel/internal/testing"
	"io"
	"log"
	"math/rand"
	"strings"
	"testing"
	"time"

	_ "github.com/anacrolix/envpprof"
	qt "github.com/frankban/quicktest"
	sqlite "github.com/go-llsqlite/adapter"
	"golang.org/x/sync/errgroup"

	"github.com/anacrolix/squirrel"
)

func init() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	log.SetPrefix("std log: ")
}

func TestBlobWriteOutOfBounds(t *testing.T) {
	c := qt.New(t)
	cache := squirrel.TestingNewCache(c, squirrel.NewCacheOpts{})
	_, err := cache.OpenPinnedReadOnly("greeting")
	c.Check(err, qt.ErrorIs, squirrel.ErrNotFound)
	b := cache.BlobWithLength("greeting", 6)
	n, err := b.WriteAt([]byte("hello "), 0)
	c.Assert(err, qt.IsNil)
	c.Check(n, qt.Equals, 6)
	n, err = b.WriteAt([]byte("world\n"), 6)
	c.Check(err, qt.IsNotNil)
	c.Check(n, qt.Equals, 0)
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
	t.Skipf("this test depends on access times being updated during a transaction")
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
	closeWait := make(chan struct{})
	var writeTime time.Time
	eg, _ := errgroup.WithContext(context.Background())
	// Start a read transaction.
	eg.Go(func() error {
		return c1.Tx(func(tx *squirrel.Tx) error {
			writePb, err := tx.OpenPinned(defaultKey)
			qtc.Assert(err, qt.IsNil)
			defer writePb.Close()
			// Upgrade to a write.
			_, err = writePb.WriteAt(defaultValue, 0)
			qtc.Assert(putValue, qt.Not(qt.DeepEquals), defaultValue)
			qtc.Assert(err, qt.IsNil)
			writeTime, err = writePb.LastUsed()
			qtc.Assert(err, qt.IsNil)
			qtc.Assert(writeTime, qt.Not(qt.Equals), putTime)
			qtc.Assert(err, qt.IsNil)
			t.Logf("write time: %v", writeTime.UnixMilli())
			<-closeWait
			return writePb.Close()
		})
	})

	// Check we read the put without error, despite a write transaction being held open by writePb.
	testReadOnlyPinned(qtc, c2, defaultKey, putValue, putTime, false)
	// Signal the Tx to complete.
	close(closeWait)
	// Wait for the Tx to have completed.
	qtc.Check(eg.Wait(), qt.IsNil)
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
	qtc.Assert(err, qt.Satisfies, squirrelTesting.EofOrNil)
	qtc.Check(b2, qt.DeepEquals, value)
	afterRead, err := r2.LastUsed()
	qtc.Assert(err, qt.IsNil)
	if expectAccessUpdate {
		qtc.Check(afterRead.UnixMilli(), qt.Not(qt.Equals), beforeRead.UnixMilli())
	} else {
		qtc.Check(afterRead.UnixMilli(), qt.Equals, beforeRead.UnixMilli())
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

	openSecondCache := make(chan struct{})
	completeTx := make(chan struct{})
	// Start a read transaction.
	eg, _ := errgroup.WithContext(context.Background())
	eg.Go(func() error {
		return c1.Tx(func(tx *squirrel.Tx) error {
			writePb, err := tx.OpenPinned(defaultKey)
			qtc.Assert(err, qt.IsNil)
			defer writePb.Close()
			// Upgrade to a write.
			_, err = writePb.WriteAt(defaultValue, 0)
			qtc.Assert(putValue, qt.Not(qt.DeepEquals), defaultValue)
			qtc.Assert(err, qt.IsNil)
			close(openSecondCache)
			// Wait here until initializing another cache instance blocks.
			<-completeTx
			return writePb.Close()
		})
	})

	<-openSecondCache
	// This will cause NewCache to trigger the write Tx above to complete, thereby unblocking it.
	cacheOpts.ConnBlockedOnBusy = &completeTx
	c2 := squirrel.TestingNewCache(qtc, cacheOpts)
	qtc.Check(c2.Close(), qt.IsNil)
	qtc.Check(eg.Wait(), qt.IsNil)
}

func TestTxWhileOpenedPinnedBlob(t *testing.T) {
	qtc := qt.New(t)
	cacheOpts := squirrel.TestingDefaultCacheOpts(qtc)
	cacheOpts.SetJournalMode = "wal"
	cache := squirrel.TestingNewCache(qtc, cacheOpts)
	err := cache.Put(defaultKey, defaultValue)
	qtc.Assert(err, qt.IsNil)
	pb, err := cache.OpenPinnedReadOnly(defaultKey)
	qtc.Assert(err, qt.IsNil)
	eg, _ := errgroup.WithContext(context.Background())
	txStarted := make(chan struct{})
	eg.Go(func() error {
		return cache.Tx(func(tx *squirrel.Tx) error {
			close(txStarted)
			return tx.Delete(defaultKey)
		})
	})
	<-txStarted
	b, err := io.ReadAll(io.NewSectionReader(pb, 0, pb.Length()))
	qtc.Assert(err, qt.Satisfies, squirrelTesting.EofOrNil)
	qtc.Assert(b, qt.DeepEquals, defaultValue)
	err = pb.Close()
	qtc.Assert(err, qt.IsNil)
	err = eg.Wait()
	qtc.Assert(err, qt.IsNil)
	pb, err = cache.OpenPinnedReadOnly(defaultKey)
	qtc.Assert(err, qt.ErrorIs, squirrel.ErrNotFound)
}

func TestWriteVeryLargeBlob(t *testing.T) {
	qtc := qt.New(t)
	cacheOpts := squirrel.TestingDefaultCacheOpts(qtc)
	cache := squirrel.TestingNewCache(qtc, cacheOpts)
	source := rand.NewSource(1)
	randRdr := rand.New(source)
	const valueLen int64 = 1 << 30
	blob, err := cache.Create(defaultKey, squirrel.CreateOpts{valueLen})
	qtc.Assert(err, qt.IsNil)
	h := newFastestHash()
	n, _ := io.Copy(io.MultiWriter(io.NewOffsetWriter(blob, 0), h), randRdr)
	qtc.Assert(n, qt.Equals, valueLen)
	qtc.Assert(blob.Close(), qt.IsNil)
	readHash := newFastestHash()
	blob, err = cache.OpenPinnedReadOnly(defaultKey)
	qtc.Assert(err, qt.IsNil)
	defer blob.Close()
	n, err = io.Copy(readHash, io.NewSectionReader(blob, 0, valueLen))
	qtc.Check(err, qt.IsNil)
	qtc.Assert(n, qt.Equals, valueLen)
	qtc.Assert(h.Sum32(), qt.Equals, readHash.Sum32())
}

func TestIterBlobsWithHigherCachedBlobs(t *testing.T) {
	qtc := qt.New(t)
	cacheOpts := squirrel.TestingDefaultCacheOpts(qtc)
	cacheOpts.MaxBlobSize.Set(1)
	cache := squirrel.TestingNewCache(qtc, cacheOpts)
	pb, err := cache.Create(defaultKey, squirrel.CreateOpts{Length: 2})
	qtc.Assert(err, qt.IsNil)
	defer pb.Close()
	var b [2]byte
	n, err := pb.ReadAt(b[:], 1)
	qtc.Assert(err, qt.Satisfies, squirrelTesting.EofOrNil)
	qtc.Assert(n, qt.Equals, 1)
	n, err = pb.ReadAt(b[:], 0)
	qtc.Assert(err, qt.Satisfies, squirrelTesting.EofOrNil)
	qtc.Assert(n, qt.Equals, 2)
}

func TestCreateChangeSize(t *testing.T) {
	qtc := qt.New(t)
	cache := squirrel.TestingNewCache(qtc, squirrel.TestingDefaultCacheOpts(qtc))
	putViaCreate := func(key, value string) (err error) {
		pb, err := cache.Create(key, squirrel.CreateOpts{Length: int64(len(value))})
		if err != nil {
			return
		}
		defer pb.Close()
		n, err := io.CopyBuffer(io.NewOffsetWriter(pb, 0), strings.NewReader(value), make([]byte, 2))
		if err != nil {
			return
		}
		if n != int64(len(value)) {
			panic(n)
		}
		return
	}
	err := putViaCreate("hello", "world")
	qtc.Assert(err, qt.IsNil)
	err = putViaCreate("hello", "america")
	qtc.Assert(err, qt.IsNil)
	err = putViaCreate("hello", "mundo")
	qtc.Assert(err, qt.IsNil)
	value, err := cache.ReadAll("hello", nil)
	qtc.Check(err, qt.IsNil)
	qtc.Check(string(value), qt.Equals, "mundo")
}
