package squirrel

import (
	"github.com/anacrolix/generics"
)

type InitConnOpts struct {
	SetSynchronous int
	SetJournalMode string
	MmapSizeOk     bool  // If false, a package-specific default will be used.
	MmapSize       int64 // If MmapSizeOk is set, use sqlite default if < 0, otherwise this value.
	SetLockingMode string
	// Page count is limited to uint32, but this value can be negative too, or interpreted as 1024
	// byte blocks of memory. In the C code it's an int (which would be int32 in Go?).
	CacheSize generics.Option[int64]
}

// Fields are in order of how they should be used during initialization.
type InitDbOpts struct {
	SetAutoVacuum     generics.Option[string]
	RequireAutoVacuum generics.Option[any]
	PageSize          int
	DontInitSchema    bool
	NoTriggers        bool
	// If non-zero, overrides the existing setting.
	Capacity int64
}

type NewConnOpts struct {
	// See https://www.sqlite.org/c3ref/open.html. NB: "If the filename is an empty string, then a
	// private, temporary on-disk database will be created. This private database will be
	// automatically deleted as soon as the database connection is closed."
	Path   string
	Memory bool
}
