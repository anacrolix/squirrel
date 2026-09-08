package squirrel

import (
	squirrelTesting "github.com/anacrolix/squirrel/internal/testing"
	"io"
	"testing"

	"github.com/anacrolix/log"
	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
	"github.com/go-quicktest/qt"
	"golang.org/x/sync/errgroup"
)

func TestConcurrentCreateBlob(t *testing.T) {
	opts := TestingDefaultCacheOpts(t)
	opts.Capacity = -1
	logger := log.Default.WithNames("test")
	t.Logf("shared path: %q", opts.Path)
	opts.SetJournalMode = "wal"
	var eg errgroup.Group
	doPut := func(value string) func() error {
		cache := TestingNewCache(t, opts)
		logger.Levelf(log.Debug, "opened cache for %q", value)
		return func() (err error) {
			logger.Levelf(log.Debug, "putting %q", value)
			err = cache.Put("greeting", []byte(value))
			logger.Levelf(log.Debug, "put %q: %v", value, err)
			return
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
	qt.Assert(t, qt.IsNil(eg.Wait()))
	cache := TestingNewCache(t, opts)
	pb, err := cache.OpenPinnedReadOnly("greeting")
	qt.Assert(t, qt.IsNil(err))
	b, err := io.ReadAll(io.NewSectionReader(pb, 0, pb.Length()))
	qt.Check(t, qt.IsNil(pb.Close()))
	qt.Check(t, qt.Satisfies(err, squirrelTesting.EofOrNil))
	qt.Check(t, qt.SliceContains(allValues, string(b)))
	conn, err := newSqliteConn(opts.NewConnOpts)
	qt.Assert(t, qt.IsNil(err))
	defer conn.Close()
	var count setOnce[int64]
	qt.Assert(t, qt.IsNil(sqlitex.Exec(conn, "select count(*) from blobs", func(stmt *sqlite.Stmt) error {
		count.Set(stmt.ColumnInt64(0))
		return nil
	})))
	qt.Check(t, qt.Equals(count.Value(), int64(1)))
}

// Show that seeking GE past the end means Prev won't work and we have to use Last.
func TestSeekingBlobBtree(t *testing.T) {
	blobs := makeBlobCache()
	blobs.Upsert(valueKey{1, 0}, nil)
	blobs.Upsert(valueKey{1, 1}, nil)
	qt.Assert(t, qt.Equals(blobs.Len(), 2))
	it := blobs.Iterator()
	it.SeekGE(valueKey{1, 1})
	it.Prev()
	qt.Assert(t, qt.Equals(it.Cur(), valueKey{1, 0}))
	it.SeekGE(valueKey{1, 2})
	qt.Check(t, qt.IsFalse(it.Valid()))
	it.Last()
	qt.Assert(t, qt.Equals(it.Cur(), valueKey{1, 1}))
}
