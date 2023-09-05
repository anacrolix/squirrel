package squirrel

import (
	"fmt"
	qt "github.com/frankban/quicktest"
	"io"
	"testing"
)

func BenchmarkSingleKey(b *testing.B) {
	c := qt.New(b)
	cache := newCache(c, NewCacheOpts{})
	key := "hello"
	value := []byte("world")
	err := cache.Put(key, []byte("world"))
	c.Assert(err, qt.IsNil)
	b.Run("Hit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pb, err := cache.Open(key)
			if err != nil {
				b.FailNow()
			}
			var buf [6]byte
			n, err := pb.Read(buf[:])
			if err != io.EOF && err != nil {
				b.Fatalf("got error reading value from blob: %v", err)
			}
			if n != len(value) {
				err = fmt.Errorf("read unexpected length %v after %v iterations", n, i)
				b.Fatal(err)
			}
		}
	})
}
