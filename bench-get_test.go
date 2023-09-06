package squirrel

import (
	"fmt"
	qt "github.com/frankban/quicktest"
	"io"
	"testing"
)

func benchCacheGets(cache *Cache, b *testing.B) {
	c := qt.New(b)
	key := "hello"
	value := []byte("world")
	err := cache.Put(key, []byte("world"))
	c.Assert(err, qt.IsNil)
	b.Run("Hit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pb, err := cache.OpenPinned(key)
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
		err := cache.Tx(func() bool {
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
			return false
		})
		if err != nil {
			b.Fatalf("error in transaction: %v", err)
		}
	})
}

func BenchmarkNoCacheBlobs(b *testing.B) {
	c := qt.New(b)
	cacheOpts := NewCacheOpts{NoCacheBlobs: true, NoFlushBlobs: true}
	//cacheOpts.Path = "here.db"
	cache := newCache(c, cacheOpts)
	benchCacheGets(cache, b)
}

func BenchmarkCacheDefaults(b *testing.B) {
	c := qt.New(b)
	cacheOpts := NewCacheOpts{NoFlushBlobs: true}
	//cacheOpts.Path = "here.db"
	cache := newCache(c, cacheOpts)
	benchCacheGets(cache, b)
}
