package squirrel_test

import (
	"math/rand"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/anacrolix/squirrel"
)

func benchCache(
	b *testing.B,
	cacheOpts squirrel.NewCacheOpts,
	setup func(cache *squirrel.Cache) error,
	loop func(cache *squirrel.Cache) error,
) {
	c := qt.New(b)
	cache := squirrel.TestingNewCache(c, cacheOpts)
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

func benchCacheWrapLoop(
	b *testing.B,
	cacheOpts squirrel.NewCacheOpts,
	setup func(cache *squirrel.Cache) error,
	loop func(cache *squirrel.Cache) error,
) {
	c := qt.New(b)
	cache := squirrel.TestingNewCache(c, cacheOpts)
	err := setup(cache)
	c.Assert(err, qt.IsNil)
	b.ResetTimer()
	err = loop(cache)
	b.StopTimer()
	c.Assert(err, qt.IsNil)
}

const defaultKey = "hello"

var defaultValue = []byte("world")

func readRandSlow(b []byte) {
	n, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	if n != len(b) {
		panic(n)
	}
}

func readRandSparse(b []byte) {
	for i := 0; i < 10; i++ {
		b[rand.Intn(len(b))] = byte(i)
	}
}
