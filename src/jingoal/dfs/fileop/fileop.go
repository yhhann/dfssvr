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

	// Duplicate duplicates an entry for a file.
	Duplicate(oid string) (string, error)

	// Remove deletes a file by its id.
	Remove(id string, domain int64) (bool, *FileMeta, error)

	// Close releases resources the handler holds.
	Close() error

	// Find finds a file, if the file not exists, return empty string.
	// If the file exists, return its file id.
	// If the file exists and is a duplication, return its primitive file id.
	Find(fid string) (string, error)

	// Name returns handler's name.
	Name() string

	// HandlerType returns type of the handler.
	HandlerType() HandlerType

	// IsHealthy checks whether shard is ok.
	IsHealthy() bool

	// FindByMd5 finds a file by its md5.
	FindByMd5(md5 string, domain int64, size int64) (string, error)
}
