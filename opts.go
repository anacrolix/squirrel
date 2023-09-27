package squirrel

import (
	g "github.com/anacrolix/generics"
)

type InitConnOpts struct {
	SetSynchronous int
	SetJournalMode string
	MmapSizeOk     bool  // If false, a package-specific default will be used.
	MmapSize       int64 // If MmapSizeOk is set, use sqlite default if < 0, otherwise this value.
	SetLockingMode string
	// Applies sqlite3 pragma cache_size. If negative it's the number of kibibytes. If positive,
	// it's the number of pages. int64 might be too large for the true range of values permissible.
	CacheSize g.Option[int64]
	// Maximum length of a blob or text value.
	LengthLimit      g.Option[int]
	JournalSizeLimit g.Option[int64]
}

// Fields are in order of how they should be used during initialization.
type InitDbOpts struct {
	SetAutoVacuum     g.Option[string]
	RequireAutoVacuum g.Option[any]
	PageSize          int
	DontInitSchema    bool
	NoTriggers        bool
	// If non-zero, overrides the existing setting. Less than zero is unlimited.
	Capacity int64
}

type NewConnOpts struct {
	// See https://www.sqlite.org/c3ref/open.html. NB: "If the filename is an empty string, then a
	// private, temporary on-disk database will be created. This private database will be
	// automatically deleted as soon as the database connection is closed."
	Path   string
	Memory bool
	// sqlite3 has a default limit of 1GB. Due to integer types used internally, I think it's not
	// possible to go over 2GiB-1.
	MaxBlobSize g.Option[maxBlobSizeType]
}
