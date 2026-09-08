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
	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-quicktest/qt"
	"golang.org/x/sync/errgroup"

	"github.com/anacrolix/squirrel"
)

func init() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	log.SetPrefix("std log: ")
}

func TestBlobWriteOutOfBounds(t *testing.T) {
	cache := squirrel.TestingNewCache(t, squirrel.NewCacheOpts{})
	_, err := cache.OpenPinnedReadOnly("greeting")
	qt.Check(t, qt.ErrorIs(err, squirrel.ErrNotFound))
	b := cache.BlobWithLength("greeting", 6)
	n, err := b.WriteAt([]byte("hello "), 0)
	qt.Assert(t, qt.IsNil(err))
	qt.Check(t, qt.Equals(n, 6))
	n, err = b.WriteAt([]byte("world\n"), 6)
	qt.Check(t, qt.IsNotNil(err))
	qt.Check(t, qt.Equals(n, 0))
}

func TestTagDeletedWithBlob(t *testing.T) {
	opts := squirrel.NewCacheOpts{}
	opts.Capacity = 43
	cache := squirrel.TestingNewCache(t, squirrel.NewCacheOpts{})
	b := cache.OpenWithLength("hello", 42)
	b.SetTag("gender", "yes")
	qt.Assert(t, qt.IsNil(b.GetTag("gender", func(stmt *sqlite.Stmt) {
		qt.Check(t, qt.Equals(stmt.ColumnText(0), "yes"))
	})))
	b.Delete()
	var tagOk bool
	b.GetTag("gender", func(stmt *sqlite.Stmt) {
		tagOk = true
	})
	qt.Check(t, qt.IsFalse(tagOk))
}

func waitSqliteSubsec() {
	// Wait long enough for unixepoch('now', 'subsec') to change in sqlite.
	time.Sleep(2 * time.Millisecond)
}

// Check that we can read while there's a write transaction, and not error due to not being able to
// apply access.
func TestIgnoreBusyUpdatingAccessOnRead(t *testing.T) {
	t.Skipf("this test depends on access times being updated during a transaction")
	cacheOpts := squirrel.TestingDefaultCacheOpts(t)

	c1 := squirrel.TestingNewCache(t, cacheOpts)
	defer c1.Close()
	putValue := []byte("mundo")
	err := c1.Put(defaultKey, putValue)
	qt.Assert(t, qt.IsNil(err))
	putTime, err := c1.NewBlobRef(defaultKey).LastUsed()
	qt.Assert(t, qt.IsNil(err))
	t.Logf("put time: %v", putTime.UnixMilli())

	// There should be no lock on the database.
	c2 := squirrel.TestingNewCache(t, cacheOpts)

	waitSqliteSubsec()
	closeWait := make(chan struct{})
	var writeTime time.Time
	eg, _ := errgroup.WithContext(context.Background())
	// Start a read transaction.
	eg.Go(func() error {
		return c1.Tx(func(tx *squirrel.Tx) error {
			writePb, err := tx.OpenPinned(defaultKey)
			qt.Assert(t, qt.IsNil(err))
			defer writePb.Close()
			// Upgrade to a write.
			_, err = writePb.WriteAt(defaultValue, 0)
			qt.Assert(t, qt.Not(qt.DeepEquals(putValue, defaultValue)))
			qt.Assert(t, qt.IsNil(err))
			writeTime, err = writePb.LastUsed()
			qt.Assert(t, qt.IsNil(err))
			qt.Assert(t, qt.Not(qt.Equals(writeTime, putTime)))
			qt.Assert(t, qt.IsNil(err))
			t.Logf("write time: %v", writeTime.UnixMilli())
			<-closeWait
			return writePb.Close()
		})
	})

	// Check we read the put without error, despite a write transaction being held open by writePb.
	testReadOnlyPinned(t, c2, defaultKey, putValue, putTime, false)
	// Signal the Tx to complete.
	close(closeWait)
	// Wait for the Tx to have completed.
	qt.Check(t, qt.IsNil(eg.Wait()))
	// Now check that we read the new written value, and our read updates access.
	testReadOnlyPinned(t, c2, defaultKey, defaultValue, writeTime, true)

}

func testReadOnlyPinned(
	tb testing.TB,
	cache *squirrel.Cache,
	key string,
	value []byte,
	lastUsed time.Time,
	expectAccessUpdate bool,
) {
	r2, err := cache.OpenPinnedReadOnly(key)
	defer r2.Close()
	beforeRead, err := r2.LastUsed()
	qt.Check(tb, qt.Equals(beforeRead.UnixMilli(), lastUsed.UnixMilli()))
	waitSqliteSubsec()
	b2, err := io.ReadAll(io.NewSectionReader(r2, 0, r2.Length()))
	qt.Assert(tb, qt.Satisfies(err, squirrelTesting.EofOrNil))
	qt.Check(tb, qt.DeepEquals(b2, value))
	afterRead, err := r2.LastUsed()
	qt.Assert(tb, qt.IsNil(err))
	if expectAccessUpdate {
		qt.Check(tb, qt.Not(qt.Equals(afterRead.UnixMilli(), beforeRead.UnixMilli())))
	} else {
		qt.Check(tb, qt.Equals(afterRead.UnixMilli(), beforeRead.UnixMilli()))
	}
}

// Check that we can read while there's a write transaction, and not error due to not being able to
// apply access.
func TestNewCacheWaitsForWrite(t *testing.T) {
	cacheOpts := squirrel.TestingDefaultCacheOpts(t)

	c1 := squirrel.TestingNewCache(t, cacheOpts)
	defer c1.Close()
	putValue := []byte("mundo")
	err := c1.Put(defaultKey, putValue)
	qt.Assert(t, qt.IsNil(err))

	openSecondCache := make(chan struct{})
	completeTx := make(chan struct{})
	// Start a read transaction.
	eg, _ := errgroup.WithContext(context.Background())
	eg.Go(func() error {
		return c1.Tx(func(tx *squirrel.Tx) error {
			writePb, err := tx.OpenPinned(defaultKey)
			qt.Assert(t, qt.IsNil(err))
			defer writePb.Close()
			// Upgrade to a write.
			_, err = writePb.WriteAt(defaultValue, 0)
			qt.Assert(t, qt.Not(qt.DeepEquals(putValue, defaultValue)))
			qt.Assert(t, qt.IsNil(err))
			close(openSecondCache)
			// Wait here until initializing another cache instance blocks.
			<-completeTx
			return writePb.Close()
		})
	})

	<-openSecondCache
	// This will cause NewCache to trigger the write Tx above to complete, thereby unblocking it.
	cacheOpts.ConnBlockedOnBusy = &completeTx
	c2 := squirrel.TestingNewCache(t, cacheOpts)
	qt.Check(t, qt.IsNil(c2.Close()))
	qt.Check(t, qt.IsNil(eg.Wait()))
}

func TestTxWhileOpenedPinnedBlob(t *testing.T) {
	cacheOpts := squirrel.TestingDefaultCacheOpts(t)
	cacheOpts.SetJournalMode = "wal"
	cache := squirrel.TestingNewCache(t, cacheOpts)
	err := cache.Put(defaultKey, defaultValue)
	qt.Assert(t, qt.IsNil(err))
	pb, err := cache.OpenPinnedReadOnly(defaultKey)
	qt.Assert(t, qt.IsNil(err))
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
	qt.Assert(t, qt.Satisfies(err, squirrelTesting.EofOrNil))
	qt.Assert(t, qt.DeepEquals(b, defaultValue))
	err = pb.Close()
	qt.Assert(t, qt.IsNil(err))
	err = eg.Wait()
	qt.Assert(t, qt.IsNil(err))
	pb, err = cache.OpenPinnedReadOnly(defaultKey)
	qt.Assert(t, qt.ErrorIs(err, squirrel.ErrNotFound))
}

func TestWriteVeryLargeBlob(t *testing.T) {
	cacheOpts := squirrel.TestingDefaultCacheOpts(t)
	cache := squirrel.TestingNewCache(t, cacheOpts)
	source := rand.NewSource(1)
	randRdr := rand.New(source)
	const valueLen int64 = 1 << 30
	blob, err := cache.Create(defaultKey, squirrel.CreateOpts{valueLen})
	qt.Assert(t, qt.IsNil(err))
	h := newFastestHash()
	n, _ := io.Copy(io.MultiWriter(io.NewOffsetWriter(blob, 0), h), randRdr)
	qt.Assert(t, qt.Equals(n, valueLen))
	qt.Assert(t, qt.IsNil(blob.Close()))
	readHash := newFastestHash()
	blob, err = cache.OpenPinnedReadOnly(defaultKey)
	qt.Assert(t, qt.IsNil(err))
	defer blob.Close()
	n, err = io.Copy(readHash, io.NewSectionReader(blob, 0, valueLen))
	qt.Check(t, qt.IsNil(err))
	qt.Assert(t, qt.Equals(n, valueLen))
	qt.Assert(t, qt.Equals(h.Sum32(), readHash.Sum32()))
}

func TestIterBlobsWithHigherCachedBlobs(t *testing.T) {
	cacheOpts := squirrel.TestingDefaultCacheOpts(t)
	cacheOpts.MaxBlobSize.Set(1)
	cache := squirrel.TestingNewCache(t, cacheOpts)
	pb, err := cache.Create(defaultKey, squirrel.CreateOpts{Length: 2})
	qt.Assert(t, qt.IsNil(err))
	defer pb.Close()
	var b [2]byte
	n, err := pb.ReadAt(b[:], 1)
	qt.Assert(t, qt.Satisfies(err, squirrelTesting.EofOrNil))
	qt.Assert(t, qt.Equals(n, 1))
	n, err = pb.ReadAt(b[:], 0)
	qt.Assert(t, qt.Satisfies(err, squirrelTesting.EofOrNil))
	qt.Assert(t, qt.Equals(n, 2))
}

func TestCreateChangeSize(t *testing.T) {
	cache := squirrel.TestingNewCache(t, squirrel.TestingDefaultCacheOpts(t))
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
	qt.Assert(t, qt.IsNil(err))
	err = putViaCreate("hello", "america")
	qt.Assert(t, qt.IsNil(err))
	err = putViaCreate("hello", "mundo")
	qt.Assert(t, qt.IsNil(err))
	value, err := cache.ReadAll("hello", nil)
	qt.Check(t, qt.IsNil(err))
	qt.Check(t, qt.Equals(string(value), "mundo"))
}
