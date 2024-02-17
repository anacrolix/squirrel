package squirrel

import (
	"io/fs"
)

// Seems to not just be key specific, and all callers know it's squirrel and what the key is.
type errNotFound struct{}

func (e errNotFound) Error() string {
	return "not found"
}

func (e errNotFound) Is(target error) bool {
	if target == fs.ErrNotExist {
		return true
	}
	return false
}

var ErrNotFound = errNotFound{}
