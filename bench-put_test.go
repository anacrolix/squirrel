package squirrel

import (
	qt "github.com/frankban/quicktest"
	"testing"
)

func benchCachePut(cache *Cache, b *testing.B) {
	c := qt.New(b)
	key := "hello"
	value := []byte("world")
	valueLen := len(value)
	err := cache.Put(key, value)
	c.Assert(err, qt.IsNil)
	b.Run("Put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := cache.Put(key, value)
			if err != nil {
				b.Fatalf("error putting: %v", err)
			}
		}
	})
	b.Run("PinnedBlobWriteAt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pb, err := cache.OpenPinned(key)
			if err != nil {
				b.Fatalf("error putting: %v", err)
			}
			n, err := pb.WriteAt(value, 0)
			if n != valueLen {
				b.Fatal(n)
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPutDefaults(b *testing.B) {
	c := qt.New(b)
	cache := newCache(c, NewCacheOpts{NoFlushBlobs: true})
	benchCachePut(cache, b)
}

func BenchmarkPutNoBlobCaching(b *testing.B) {
	cacheOpts := defaultCacheOpts(b)
	cacheOpts.NoCacheBlobs = true
	benchCache(b,
		cacheOpts,
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		})
}

func BenchmarkPutNoBlobCachingWal(b *testing.B) {
	cacheOpts := defaultCacheOpts(b)
	cacheOpts.NoCacheBlobs = true
	cacheOpts.SetJournalMode = "wal"
	benchCache(b,
		cacheOpts,
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		})
}

func BenchmarkPutNoBlobCachingTransaction(b *testing.B) {
	cacheOpts := defaultCacheOpts(b)
	cacheOpts.NoCacheBlobs = true
	benchCacheWrapLoop(b,
		cacheOpts,
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *Cache) error {
			return cache.Tx(func() bool {
				for i := 0; i < b.N; i++ {
					err := cache.Put(defaultKey, defaultValue)
					if err != nil {
						b.Fatal(err)
					}
				}
				return true
			})
		})
}

func BenchmarkPutNoBlobCachingSynchronous(b *testing.B) {
	cacheOpts := defaultCacheOpts(b)
	cacheOpts.NoCacheBlobs = true
	cacheOpts.SetSynchronous = 1
	benchCache(b,
		cacheOpts,
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		})
}

func BenchmarkPutNoBlobCachingSynchronousWal(b *testing.B) {
	cacheOpts := defaultCacheOpts(b)
	cacheOpts.NoCacheBlobs = true
	cacheOpts.SetSynchronous = 1
	cacheOpts.SetJournalMode = "wal"
	benchCache(b,
		cacheOpts,
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		})
}

func BenchmarkPutNoBlobCachingExclusiveLockingModeTemporaryDatabase(b *testing.B) {
	cacheOpts := defaultCacheOpts(b)
	cacheOpts.NoCacheBlobs = true
	cacheOpts.SetLockingMode = "exclusive"
	cacheOpts.Path = ""
	benchCache(b,
		cacheOpts,
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		})
}

func BenchmarkPutNoBlobCachingExclusiveLockingMode(b *testing.B) {
	cacheOpts := defaultCacheOpts(b)
	cacheOpts.NoCacheBlobs = true
	cacheOpts.SetLockingMode = "exclusive"
	benchCache(b,
		cacheOpts,
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		})
}

func BenchmarkPutNoBlobCachingExclusiveLockingModeJournalModeWal(b *testing.B) {
	cacheOpts := defaultCacheOpts(b)
	cacheOpts.NoCacheBlobs = true
	cacheOpts.SetLockingMode = "exclusive"
	cacheOpts.SetJournalMode = "wal"
	benchCache(b,
		cacheOpts,
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		},
		func(cache *Cache) error {
			return cache.Put(defaultKey, defaultValue)
		})
}
