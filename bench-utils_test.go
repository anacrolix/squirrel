package squirrel

import (
	qt "github.com/frankban/quicktest"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tempCachePath(c testing.TB) string {
	return filepath.Join(c.TempDir(), "squirrel.db")
}

func newCachePath(c *qt.C, dir string) string {
	c.Assert(os.MkdirAll(dir, 0o700), qt.IsNil)
	file, err := os.CreateTemp(dir, "")
	c.Assert(err, qt.IsNil)
	file.Close()
	return file.Name()
}

func benchCache(b *testing.B, cacheOpts NewCacheOpts, setup func(cache *Cache) error, loop func(cache *Cache) error) {
	c := qt.New(b)
	cache := newCache(c, cacheOpts)
	err := setup(cache)
	c.Assert(err, qt.IsNil)
	started := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = loop(cache)
		if err != nil {
			c.Fatalf("error in iteration %v after %v: %v", i, time.Since(started), err)
		}
	}
}

func benchCacheWrapLoop(b *testing.B, cacheOpts NewCacheOpts, setup func(cache *Cache) error, loop func(cache *Cache) error) {
	c := qt.New(b)
	cache := newCache(c, cacheOpts)
	err := setup(cache)
	c.Assert(err, qt.IsNil)
	b.ResetTimer()
	err = loop(cache)
	c.Assert(err, qt.IsNil)
}

func defaultCacheOpts(tb testing.TB) (ret NewCacheOpts) {
	ret.Path = tempCachePath(tb)
	ret.PageSize = 4096
	return
}

const defaultKey = "hello"

var defaultValue = []byte("world")

func readRand(b []byte) {
	n, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	if n != len(b) {
		panic(n)
	}
}
