package squirrel_test

import (
	"hash"
	"hash/adler32"
	"hash/crc32"
	"hash/fnv"
	"testing"
)

var newFastestHash = crc32.NewIEEE

func BenchmarkHashes(b *testing.B) {
	b.Skip("not usually worth running")
	const bufSize = 4096
	benchHash := func(name string, h hash.Hash) {
		b.Run(name, func(b *testing.B) {
			var buf [bufSize]byte
			b.SetBytes(int64(len(buf)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Write(buf[:])
			}
		})
	}
	benchHash("Crc32", crc32.NewIEEE())
	benchHash("Adler32", adler32.New())
	benchHash("Fnv-32", fnv.New32())
}
