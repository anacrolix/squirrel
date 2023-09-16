package squirrel_test

import (
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
