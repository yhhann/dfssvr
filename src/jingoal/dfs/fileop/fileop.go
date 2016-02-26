// Package fileop processes the file storage biz.
package fileop

import (
	"io"

	"jingoal/dfs/transfer"
)

const (
	DefaultChunkSizeInBytes = 4096
)

type dfsFileMode uint

const (
	FileModeWrite dfsFileMode = 1 << iota // For write only
	FileModeRead                          // For read only
)

type HandlerType uint

const (
	GlusterType HandlerType = 1 << iota // For gluster type
	GridFSType                          // For gridfs type
	DegradeType                         // For degrade type
)

// DFSFile represents a file of the underlying storage.
type DFSFile interface {
	io.ReadWriteCloser

	// GetFileInfo returns file meta info.
	GetFileInfo() *transfer.FileInfo
}

// DFSFileHandler represents the file handler of underlying storage.
type DFSFileHandler interface {
	// Create creates a DFSFile for write
	Create(info *transfer.FileInfo) (DFSFile, error)

	// Open opens a DFSFile for read
	Open(id string, domain int64) (DFSFile, error)

	// Remove deletes a file by its id.
	Remove(id string, domain int64) error

	// Close releases resources the handler holds.
	Close() error

	// Name returns handler's name.
	Name() string

	// HandlerType returns type of the handler.
	HandlerType() HandlerType

	// IsHealthy checks whether shard is ok.
	IsHealthy() bool
}
