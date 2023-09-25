package squirrel

type rowid = int64

type keyCols struct {
	id     rowid
	length int64
}

// sqlite3 mentions this might be limited to 2<<31-1. By default it's actually limited to 1e9. The
// type however varies between uint64 in sqlite3_result_zeroblob64, to int from sqlite3_blob_bytes.
type maxBlobSizeType = int64

type valueKey struct {
	keyId  rowid
	offset int64
}
