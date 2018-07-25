package fileop

import (
	"fmt"
)

const (
	_ = iota
	GlusterFSCreateFileError
)

var (
	NoEntityError = fmt.Errorf("no entity")
)

type CreateFileError struct {
	Code int
	Orig error
}

func (e CreateFileError) Error() string {
	return fmt.Sprintf("file error, code %d, %v", e.Code, e.Orig)
}
