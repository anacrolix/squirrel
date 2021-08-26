package squirrel

type InitConnOpts struct {
	SetSynchronous int
	SetJournalMode string
	MmapSizeOk     bool  // If false, a package-specific default will be used.
	MmapSize       int64 // If MmapSizeOk is set, use sqlite default if < 0, otherwise this value.
}

type InitDbOpts struct {
	DontInitSchema bool
	PageSize       int
	// If non-zero, overrides the existing setting.
	Capacity   int64
	NoTriggers bool
}

type NewConnOpts struct {
	// See https://www.sqlite.org/c3ref/open.html. NB: "If the filename is an empty string, then a
	// private, temporary on-disk database will be created. This private database will be
	// automatically deleted as soon as the database connection is closed."
	Path   string
	Memory bool
	// Whether multiple blobs will not be read simultaneously. Enables journal mode other than WAL,
	// and NumConns < 2.
	NoConcurrentBlobReads bool
}
