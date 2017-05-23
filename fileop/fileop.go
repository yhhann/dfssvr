// Package fileop processes the file storage biz.
package fileop

import (
	"io"

	"jingoal.com/dfs/proto/transfer"
)

type dfsFileMode uint

const (
	FileModeWrite dfsFileMode = 1 << iota // For write only
	FileModeRead                          // For read only
)

const (
	HealthOk int = iota
	MetaNotHealthy
	StoreNotHealthy
)

type DFSFileMeta struct {
	Bizname   string `bson:"bizname"`
	Fid       string `bson:"weedfid"`
	ChunkSize int64  `bson:"chunksize"`
}

// DFSFile represents a file of the underlying storage.
type DFSFile interface {
	io.ReadWriteCloser

	// GetFileInfo returns file info.
	GetFileInfo() *transfer.FileInfo

	// updateFileMeta updates file dfs meta.
	updateFileMeta(map[string]interface{})

	// getFileMeta returns file dfs meta.
	getFileMeta() *DFSFileMeta
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
	Find(fid string) (string, *DFSFileMeta, *transfer.FileInfo, error)

	// Name returns handler's name.
	Name() string

	// IsHealthy checks whether shard is ok.
	IsHealthy() bool

	// HealthStatus returns the status of node health.
	HealthStatus() int

	// FindByMd5 finds a file by its md5.
	FindByMd5(md5 string, domain int64, size int64) (string, error)
}
