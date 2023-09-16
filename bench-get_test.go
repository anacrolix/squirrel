package squirrel_test

import (
	"fmt"
	"io"
	"testing"

	g "github.com/anacrolix/generics"
	qt "github.com/frankban/quicktest"

	"github.com/anacrolix/squirrel"
)

func benchCacheGets(cache *squirrel.Cache, b *testing.B) {
	c := qt.New(b)
	key := "hello"
	value := []byte("world")
	err := cache.Put(key, []byte("world"))
	c.Assert(err, qt.IsNil)
	b.Run("Hit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pb, err := cache.OpenPinnedReadOnly(key)
			if err != nil {
				b.Fatalf("error opening cache: %v", err)
			}
			var buf [6]byte
			n, err := pb.ReadAt(buf[:], 0)
			pb.Close()
			if err != io.EOF && err != nil {
				b.Fatalf("got error reading value from blob on iteration %v: %v", i, err)
			}
			if n != len(value) {
				err = fmt.Errorf("read unexpected length %v after %v iterations", n, i)
				b.Fatal(err)
			}
		}
	})
	b.Run("HitFull", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf [6]byte
			n, err := cache.ReadFull(key, buf[:])
			if err != nil {
				b.Fatalf("got error reading value from blob on iteration %v: %v", i, err)
			}
			if n != len(value) {
				err = fmt.Errorf("read unexpected length %v after %v iterations", n, i)
				b.Fatal(err)
			}
		}
	})
	b.Run("HitFullTransaction", func(b *testing.B) {
		err := cache.Tx(func(tx *squirrel.Tx) error {
			for i := 0; i < b.N; i++ {
				var buf [6]byte
				n, err := tx.ReadFull(key, buf[:])
				if err != nil {
					b.Fatalf("got error reading value from blob on iteration %v: %v", i, err)
				}
				if n != len(value) {
					err = fmt.Errorf("read unexpected length %v after %v iterations", n, i)
					b.Fatal(err)
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("error in transaction: %v", err)
		}
	})
}

func BenchmarkCacheDefaults(b *testing.B) {
	c := qt.New(b)
	cacheOpts := squirrel.TestingDefaultCacheOpts(b)
	cache := squirrel.TestingNewCache(c, cacheOpts)
	benchCacheGets(cache, b)
}

func benchmarkReadAtEndOfBlob(b *testing.B, blobSize int, readSize int, cacheOpts squirrel.NewCacheOpts) {
	value := make([]byte, blobSize)
	readRandSparse(value)
	buf := make([]byte, readSize)
	b.SetBytes(int64(readSize))
	benchCache(b, cacheOpts,
		func(cache *squirrel.Cache) error {
			return cache.Put(defaultKey, value)
		},
		func(cache *squirrel.Cache) (err error) {
			pb, err := cache.OpenPinnedReadOnly(defaultKey)
			if err != nil {
				return err
			}
			defer pb.Close()
			n, err := pb.ReadAt(buf, int64(len(value)-len(buf)))
			if err != nil && err != io.EOF {
				return
			}
			if n != len(buf) {
				panic(n)
			}
			return nil
		})
}

// Demonstrate that reads from the end of blobs is slow without overflow caching, or pointer maps
// enabled.
func BenchmarkReadAtEndOfBlob(b *testing.B) {
	autoVacuums := []nestedBench{
		{"AutoVacuumNone", func(opts *squirrel.NewCacheOpts) {
			opts.SetAutoVacuum = g.Some("none")
			opts.RequireAutoVacuum = g.Some[any](0)
		}},
		{"AutoVacuumIncremental", func(opts *squirrel.NewCacheOpts) {
			// Show that incremental still generates a "pointer map" in sqlite3.
			opts.SetAutoVacuum = g.Some("incremental")
			opts.RequireAutoVacuum = g.Some[any](2)
		}},
		{"AutoVacuumFull", func(opts *squirrel.NewCacheOpts) {
			opts.SetAutoVacuum = g.Some("full")
			opts.RequireAutoVacuum = g.Some[any](1)
		}},
	}
	startNestedBenchmark(b,
		func(b testing.TB) squirrel.NewCacheOpts {
			cacheOpts := squirrel.TestingDefaultCacheOpts(b)
			// This needs to be significantly less than the blob size or the linked list of pages
			// will be cached.
			cacheOpts.CacheSize = g.Some[int64](-1 << 10) // 1 MiB
			return cacheOpts
		},
		func(b *testing.B, opts func() squirrel.NewCacheOpts) {
			benchmarkReadAtEndOfBlob(b, 4<<20, 4<<10, opts())
		},
		autoVacuums)
}

type nestedBench struct {
	name     string
	withOpts func(opts *squirrel.NewCacheOpts)
}

func startNestedBenchmark(
	b *testing.B,
	newCacheOpts func(b testing.TB) squirrel.NewCacheOpts,
	finally func(b *testing.B, opts func() squirrel.NewCacheOpts),
	nested ...[]nestedBench,
) {
	runNested(b, nil, newCacheOpts, nested, finally)
}

func runNested(
	b *testing.B,
	withOpts []func(*squirrel.NewCacheOpts),
	newCacheOpts func(testing.TB) squirrel.NewCacheOpts,
	nested [][]nestedBench,
	finally func(b *testing.B, opts func() squirrel.NewCacheOpts),
) {
	if len(nested) == 0 {
		finally(b, func() squirrel.NewCacheOpts {
			cacheOpts := newCacheOpts(b)
			for _, withOpt := range withOpts {
				withOpt(&cacheOpts)
			}
			return cacheOpts
		})
		return
	}
	for _, n := range nested[0] {
		b.Run(n.name, func(b *testing.B) {
			runNested(b, append(withOpts, n.withOpts), newCacheOpts, nested[1:], finally)
		})
	}
}
