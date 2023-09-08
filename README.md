# squirrel

[![Go Reference](https://pkg.go.dev/badge/github.com/anacrolix/squirrel.svg)](https://pkg.go.dev/github.com/anacrolix/squirrel)

squirrel is a cache backed by SQLite3. It inherits a lot of features from SQLite, including different journal modes, running in memory or on disk etc. The cache size can be adjusted on the fly.

squirrel was originally extracted from the [sqlite storage](https://github.com/anacrolix/torrent/tree/master/storage/sqlite) "direct" backend in [anacrolix/torrent].

## Benchmarks

Benchmarks are from use in [anacrolix/torrent]:

    $ go test -bench . -run @ ./storage/sqlite
    goos: darwin
    goarch: amd64
    pkg: github.com/anacrolix/torrent/storage/sqlite
    cpu: Intel(R) Core(TM) i7-8850H CPU @ 2.60GHz
    BenchmarkMarkComplete/CustomDirect/Default-12 	      14	  75241733 ns/op	 445.96 MB/s
    BenchmarkMarkComplete/Memory=false/Direct/JournalMode=/MmapSize=default-12         	      16	  77173756 ns/op	 434.79 MB/s
    BenchmarkMarkComplete/Memory=false/Direct/JournalMode=wal/MmapSize=default-12      	      14	  78494029 ns/op	 427.48 MB/s
    BenchmarkMarkComplete/Memory=false/Direct/JournalMode=off/MmapSize=default-12      	      18	  64380892 ns/op	 521.19 MB/s
    BenchmarkMarkComplete/Memory=false/Direct/JournalMode=truncate/MmapSize=default-12 	      16	  72059937 ns/op	 465.65 MB/s
    BenchmarkMarkComplete/Memory=false/Direct/JournalMode=delete/MmapSize=default-12   	      16	  72333480 ns/op	 463.89 MB/s
    BenchmarkMarkComplete/Memory=false/Direct/JournalMode=persist/MmapSize=default-12  	      15	  72494113 ns/op	 462.86 MB/s
    BenchmarkMarkComplete/Memory=false/Direct/JournalMode=memory/MmapSize=default-12   	      14	  81499105 ns/op	 411.72 MB/s
    BenchmarkMarkComplete/Memory=true/Direct/JournalMode=/MmapSize=default-12          	      20	  62141680 ns/op	 539.97 MB/s
    BenchmarkMarkComplete/Memory=true/Direct/JournalMode=off/MmapSize=default-12       	      30	  41396334 ns/op	 810.57 MB/s
    BenchmarkMarkComplete/Memory=true/Direct/JournalMode=memory/MmapSize=default-12    	      18	  58615857 ns/op	 572.45 MB/s
    PASS
    ok  	github.com/anacrolix/torrent/storage/sqlite	23.607s

## TODO

 * Tidy up the API.
 * Support transactions?
 * Add an eviction feed?
 * Update times on read, amortize costs by batching updates and flush before executing cache trimming.
 * Separate Cache and Conn types? This might allow opening extra conns while other writes are ongoing. It's unclear if there's any performance gain to be had since this was tried with the "provider" connection-pool implementations in anacrolix/torrent previously. It might also not play well with tracking blob usage timestamps.

 ## Ideas

 * Look into compile time option SQLITE_DIRECT_OVERFLOW_READ.
 * Avoid using incremental blob I/O for full writes.
 * Put value blobs in a separate table.
 * Use ints or floats for access times.
 * Add/support expiries.
 * Use auto vacuum and pragma page_count.
 * Add transaction support.
 * Use incremental blob reopen to avoid recreating cursors and other overhead in registering incremental blobs. Possibly one per connection.

## sqlite3 Notes

 * If auto_vacuum is not none, sqlite3 includes pointer map pages which makes reading at arbitrary offsets in blobs significantly faster.
 * Having lots of outstanding incremental blob handles is expensive as they are tracked in a linked list of cursors and get invalidated in certain circumstances.
 * Journal mode delete is almost as fast as WAL but only if the locking mode is exclusive.

[anacrolix/torrent]: (https://github.com/anacrolix/torrent)
