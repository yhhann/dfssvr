package fileop

import (
	"fmt"
	"log"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal/dfs/metadata"
	"jingoal/dfs/transfer"
	"jingoal/dfs/util"
)

// GridFsHandler implements DFSFileHandler.
type GridFsHandler struct {
	*metadata.Shard

	gridfs  *mgo.GridFS
	session *mgo.Session
}

// Name returns handler's name.
func (h *GridFsHandler) Name() string {
	return h.Shard.Name
}

// Create creates a DFSFile for write with the given file info.
func (h *GridFsHandler) Create(info *transfer.FileInfo) (DFSFile, error) {
	file, err := h.gridfs.Create(info.Name)
	if err != nil {
		return nil, err
	}

	// For compatible with dfs 1.0.
	// This is a bug of driver in go, chunk size in java driver is 256k,
	// but in go is 255k. So we must reset it to 256k.
	file.SetChunkSize(256 * 1024)

	oid, ok := file.Id().(bson.ObjectId)
	if !ok {
		return nil, fmt.Errorf("id %v is not an ObjectId", file.Id())
	}

	// Make a copy of file info to hold information of file.
	inf := *info
	inf.Id = oid.Hex()

	return &GridFsFile{
		GridFile: file,
		info:     &inf,
		handler:  h,
		mode:     FileModeWrite,
	}, nil
}

// Open opens a DFSFile for read with given id and domain.
func (h *GridFsHandler) Open(id string, domain int64) (DFSFile, error) {
	file, err := h.gridfs.OpenId(bson.ObjectIdHex(id))
	if err != nil {
		return nil, err
	}

	inf := &transfer.FileInfo{
		Id:     id,
		Domain: domain,
		Name:   file.Name(),
		Size:   file.Size(),
		Md5:    file.MD5(),
	}

	dfsFile := &GridFsFile{
		GridFile: file,
		info:     inf,
		handler:  h,
		mode:     FileModeRead,
	}

	return dfsFile, nil
}

// Find finds a file, if the file not exists, return empty string.
// If the file exists, return its file id.
// If the file exists and is a duplication, return its primitive file id.
func (h *GridFsHandler) Find(fid string) (string, error) {
	var id string
	if util.IsDuplId(fid) {
		// TODO:(hanyh) to find a dupl file.
		// id = ...
	} else {
		id = fid
	}

	file, err := h.gridfs.OpenId(bson.ObjectIdHex(id))
	if err == mgo.ErrNotFound {
		return "", nil
	}
	if err != nil {
		log.Printf("Failed to find file %s", fid)
		return "", err
	}

	oid, ok := file.Id().(bson.ObjectId)
	if !ok {
		log.Printf("Failed to find file %s", fid)
		return "", fmt.Errorf("find file error %s", fid)
	}

	log.Printf("Succeeded to find file %s, return %s", fid, oid.Hex())

	return oid.Hex(), nil
}

// Remove deletes a file with its id and domain.
func (h *GridFsHandler) Remove(id string, domain int64) error {
	return h.gridfs.RemoveId(bson.ObjectIdHex(id))
}

// Close releases resources the handler holds.
func (h *GridFsHandler) Close() error {
	h.session.Close()
	return nil
}

// HandlerType returns type of the handler.
func (h *GridFsHandler) HandlerType() HandlerType {
	return GridFSType
}

// IsHealthy checks whether shard is ok.
func (h *GridFsHandler) IsHealthy() bool {
	err := h.session.Run("serverStatus", nil)
	return err == nil
}

// NewGridFsHandler returns a handler for processing Grid files.
func NewGridFsHandler(shardInfo *metadata.Shard) (*GridFsHandler, error) {
	handler := &GridFsHandler{
		Shard: shardInfo,
	}

	session, err := metadata.OpenMongoSession(shardInfo.Uri)
	if err != nil {
		return nil, err
	}

	handler.session = session
	handler.gridfs = session.Copy().DB(shardInfo.Name).GridFS("fs")

	return handler, nil
}

// GridFsFile implements DFSFile.
type GridFsFile struct {
	*mgo.GridFile
	info    *transfer.FileInfo
	mode    dfsFileMode
	handler *GridFsHandler
}

// GetFileInfo returns file meta info.
func (f GridFsFile) GetFileInfo() *transfer.FileInfo {
	return f.info
}

// Close closes GridFsFile.
func (f *GridFsFile) Close() error {
	if err := f.GridFile.Close(); err != nil {
		return err
	}

	if f.mode == FileModeWrite {
		return f.updateGridMetadata()
	}
	return nil
}

func (f GridFsFile) updateGridMetadata() error {
	var opdata bson.D

	opdata = append(opdata, bson.DocElem{
		"domain", f.info.Domain,
	})
	opdata = append(opdata, bson.DocElem{
		"userid", fmt.Sprintf("%d", f.info.User),
	})
	opdata = append(opdata, bson.DocElem{
		"bizname", "dfs", // For compatible with dfs 1.0
	})
	opdata = append(opdata, bson.DocElem{
		"contentType", nil, // For compatible with dfs 1.0
	})
	opdata = append(opdata, bson.DocElem{
		"aliases", nil, // For compatible with dfs 1.0
	})

	return f.handler.gridfs.Files.Update(
		bson.M{
			"_id": bson.ObjectIdHex(f.info.Id),
		},
		bson.M{
			"$set": opdata,
		},
	)
}
