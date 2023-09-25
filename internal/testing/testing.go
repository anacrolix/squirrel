package squirrelTesting

import (
	"errors"
	"io"
)

func EofOrNil(err error) bool {
	return err == nil || errors.Is(err, io.EOF)
}
