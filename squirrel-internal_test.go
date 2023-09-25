package squirrel

import (
	squirrelTesting "github.com/anacrolix/squirrel/internal/testing"
	"io"
	"testing"

	"github.com/anacrolix/log"
	qt "github.com/frankban/quicktest"
	sqlite "github.com/go-llsqlite/adapter"
	"github.com/go-llsqlite/adapter/sqlitex"
	"golang.org/x/sync/errgroup"
)

func TestConcurrentCreateBlob(t *testing.T) {
	c := qt.New(t)
	opts := TestingDefaultCacheOpts(c)
	opts.Capacity = -1
	logger := log.Default.WithNames("test")
	t.Logf("shared path: %q", opts.Path)
	opts.SetJournalMode = "wal"
	var eg errgroup.Group
	doPut := func(value string) func() error {
		cache := TestingNewCache(c, opts)
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
	c.Assert(eg.Wait(), qt.IsNil)
	cache := TestingNewCache(c, opts)
	pb, err := cache.OpenPinnedReadOnly("greeting")
	c.Assert(err, qt.IsNil)
	b, err := io.ReadAll(io.NewSectionReader(pb, 0, pb.Length()))
	c.Check(pb.Close(), qt.IsNil)
	c.Check(err, qt.Satisfies, squirrelTesting.EofOrNil)
	c.Check(allValues, qt.Contains, string(b))
	conn, err := newSqliteConn(opts.NewConnOpts)
	c.Assert(err, qt.IsNil)
	defer conn.Close()
	var count setOnce[int64]
	c.Assert(sqlitex.Exec(conn, "select count(*) from blobs", func(stmt *sqlite.Stmt) error {
		count.Set(stmt.ColumnInt64(0))
		return nil
	}), qt.IsNil)
	c.Check(count.Value(), qt.Equals, int64(1))
}

// Show that seeking GE past the end means Prev won't work and we have to use Last.
func TestSeekingBlobBtree(t *testing.T) {
	blobs := makeBlobCache()
	blobs.Upsert(valueKey{1, 0}, nil)
	blobs.Upsert(valueKey{1, 1}, nil)
	qtc := qt.New(t)
	qtc.Assert(blobs.Len(), qt.Equals, 2)
	it := blobs.Iterator()
	it.SeekGE(valueKey{1, 1})
	it.Prev()
	qtc.Assert(it.Cur(), qt.Equals, valueKey{1, 0})
	it.SeekGE(valueKey{1, 2})
	qtc.Check(it.Valid(), qt.IsFalse)
	it.Last()
	qtc.Assert(it.Cur(), qt.Equals, valueKey{1, 1})
}
