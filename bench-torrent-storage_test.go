package squirrel

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"testing"

	qt "github.com/frankban/quicktest"
)

func BenchmarkHash(b *testing.B) {
	h := crc32.NewIEEE()
	var buf [4096]byte
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Write(buf[:])
	}
}

const defaultPieceSize = 2 << 20

func BenchmarkRandRead(b *testing.B) {
	var piece [defaultPieceSize]byte
	b.SetBytes(defaultPieceSize)
	for i := 0; i < b.N; i++ {
		readRand(piece[:])
	}
}

func BenchmarkRandReadSparse(b *testing.B) {
	var piece [defaultPieceSize]byte
	b.SetBytes(defaultPieceSize)
	for i := 0; i < b.N; i++ {
		readRandSparse(piece[:])
	}
}

var newHash = crc32.NewIEEE

func BenchmarkTorrentStorage(b *testing.B) {
	c := qt.New(b)
	cacheOpts := defaultCacheOpts(c)
	// Can't start a transaction while blobs are cached.
	cacheOpts.NoCacheBlobs = true
	cacheOpts.SetJournalMode = "wal"
	cacheOpts.Path = newCachePath(c, "testdbs")
	cacheOpts.MmapSize = 64 << 20
	cacheOpts.MmapSizeOk = true
	//cacheOpts.Capacity = 64 << 20
	cacheOpts.SetLockingMode = "exclusive"
	cacheOpts.NoTriggers = true
	b.Logf("db path: %q", cacheOpts.Path)
	const pieceSize = 2 << 20
	b.SetBytes(pieceSize)
	b.ReportMetric(1, "pieces")
	benchCache(
		b,
		cacheOpts,
		func(cache *Cache) error {
			return nil
		},
		func(cache *Cache) error {
			var key [24]byte
			readRand(key[:20])
			var piece [2 << 20]byte
			readRandSparse(piece[:])
			h0 := newHash()
			h0.Write(piece[:])
			const chunkSize = 1 << 14
			for off := uint32(0); off < pieceSize; off += chunkSize {
				binary.BigEndian.PutUint32(key[20:], off)
				err := cache.Put(string(key[:]), piece[off:off+chunkSize])
				if err != nil {
					return err
				}
			}
			h1 := newHash()
			if true {
				err := cache.Tx(func() bool {
					readAndHashBytes(b, cache, key[:], pieceSize, h1)
					return true
				})
				if err != nil {
					b.Fatal(err)
				}
			} else {
				readAndHashBytes(b, cache, key[:], pieceSize, h1)
			}
			if h0.Sum32() != h1.Sum32() {
				b.Fatal("hashes don't match")
			}
			return nil
		},
	)
}

func readAndHashBytes(b *testing.B, cache *Cache, key []byte, pieceSize uint32, hash io.Writer) {
	for off := uint32(0); off < pieceSize; off += 1 << 14 {
		binary.BigEndian.PutUint32(key[20:], off)
		var chunk [1 << 14]byte
		n, err := cache.ReadFull(string(key[:]), chunk[:])
		if err != nil {
			b.Fatal(err)
		}
		if n != len(chunk) {
			b.Fatal(n)
		}
		hash.Write(chunk[:])
	}
	for off := uint32(0); off < pieceSize; off += 1 << 14 {
		binary.BigEndian.PutUint32(key[20:], off)
		err := cache.SetTag(string(key[:]), "verified", true)
		if err != nil {
			b.Fatal(err)
		}
	}
}
