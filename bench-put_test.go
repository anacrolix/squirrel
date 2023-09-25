package squirrel_test

import (
	"io"
	"math/rand"
	"testing"

	"github.com/anacrolix/squirrel"
)

func benchmarkPutSmallItem(b *testing.B, cacheOpts func() squirrel.NewCacheOpts) {
	benchCache(b,
		cacheOpts(),
		func(cache *squirrel.Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *squirrel.Cache) error {
			return cache.Put(defaultKey, defaultValue)
		})
	b.SetBytes(int64(len(defaultValue)))
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "puts/s")
}

func BenchmarkPutSmallItem(b *testing.B) {
	startNestedBenchmark(
		b,
		squirrel.TestingDefaultCacheOpts,
		benchmarkPutSmallItem,
		[]nestedBench{
			{"Wal", func(opts *squirrel.NewCacheOpts) {
				opts.SetJournalMode = "wal"
			}},
			{"DefaultJournalMode", func(opts *squirrel.NewCacheOpts) {

			}},
		},
		[]nestedBench{
			{"LockingModeExclusive", func(opts *squirrel.NewCacheOpts) {
				opts.SetLockingMode = "exclusive"
			}},
			{"NormalLockingMode", func(opts *squirrel.NewCacheOpts) {
				opts.SetLockingMode = "normal"
			}},
		},
		[]nestedBench{
			{"NoPath", func(opts *squirrel.NewCacheOpts) {
				opts.Path = ""
			}},
			{"RegularFile", func(opts *squirrel.NewCacheOpts) {
				opts.Path = squirrel.TestingTempCachePath(b)
			}},
			{"Memory", func(opts *squirrel.NewCacheOpts) {
				opts.Memory = true
			}},
		},
		[]nestedBench{
			{"SynchronousOff", func(opts *squirrel.NewCacheOpts) {
				opts.SetSynchronous = 0
			}},
			{"SynchronousNormal", func(opts *squirrel.NewCacheOpts) {
				opts.SetSynchronous = 1
			}},
		},
	)
}

func BenchmarkTransaction(b *testing.B) {
	cacheOpts := squirrel.TestingDefaultCacheOpts(b)
	benchCacheWrapLoop(b,
		cacheOpts,
		func(cache *squirrel.Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *squirrel.Cache) error {
			return cache.Tx(func(tx *squirrel.Tx) error {
				for i := 0; i < b.N; i++ {
					err := tx.Put(defaultKey, defaultValue)
					if err != nil {
						b.Fatal(err)
					}
				}
				return nil
			})
		})
}

func BenchmarkWriteVeryLargeBlob(b *testing.B) {
	const valueLen = 1e9 + 1
	b.SetBytes(valueLen)
	writeLargeValue := func(cache *squirrel.Cache) (err error) {
		item, err := cache.Create(defaultKey, squirrel.CreateOpts{valueLen})
		if err != nil {
			return
		}
		defer item.Close()
		src := rand.New(rand.NewSource(1))
		n, err := io.CopyN(io.NewOffsetWriter(item, 0), src, valueLen)
		if n != valueLen {
			panic(n)
		}
		return
	}
	benchCache(
		b,
		squirrel.TestingDefaultCacheOpts(b),
		writeLargeValue,
		writeLargeValue,
	)
}
