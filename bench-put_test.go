package squirrel

import (
	"testing"
)

func benchmarkPutSmallItem(b *testing.B, cacheOpts func() NewCacheOpts) {
	benchCache(b,
		cacheOpts(),
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		})
	b.SetBytes(int64(len(defaultValue)))
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "puts/s")
}

func BenchmarkPutSmallItem(b *testing.B) {
	startNestedBenchmark(
		b,
		defaultCacheOpts,
		benchmarkPutSmallItem,
		[]nestedBench{
			{"Wal", func(opts *NewCacheOpts) {
				opts.SetJournalMode = "wal"
			}},
			{"DefaultJournalMode", func(opts *NewCacheOpts) {

			}},
		},
		[]nestedBench{
			{"LockingModeExclusive", func(opts *NewCacheOpts) {
				opts.SetLockingMode = "exclusive"
			}},
			{"NormalLockingMode", func(opts *NewCacheOpts) {
				opts.SetLockingMode = "normal"
			}},
		},
		[]nestedBench{
			{"NoPath", func(opts *NewCacheOpts) {
				opts.Path = ""
			}},
			{"RegularFile", func(opts *NewCacheOpts) {
				opts.Path = tempCachePath(b)
			}},
			{"Memory", func(opts *NewCacheOpts) {
				opts.Memory = true
			}},
		},
		[]nestedBench{
			{"SynchronousOff", func(opts *NewCacheOpts) {
				opts.SetSynchronous = 0
			}},
			{"SynchronousNormal", func(opts *NewCacheOpts) {
				opts.SetSynchronous = 1
			}},
		},
	)
}

func BenchmarkTransaction(b *testing.B) {
	cacheOpts := defaultCacheOpts(b)
	benchCacheWrapLoop(b,
		cacheOpts,
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *Cache) error {
			return cache.Tx(func(tx *Tx) error {
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
