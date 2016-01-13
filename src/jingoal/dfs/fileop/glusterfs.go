package fileop

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"path/filepath"

	"github.com/kshlm/gogfapi/gfapi"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal/dfs/metadata"
	"jingoal/dfs/transfer"
	"jingoal/dfs/util"
)

// GlusterHandler implements DFSFileHandler.
type GlusterHandler struct {
	*metadata.Shard
	*gfapi.Volume
	gridfs  *mgo.GridFS
	session *mgo.Session
	VolLog  string // Log file name of gluster volume
}

// initVolume initializes gluster volume.
func (h *GlusterHandler) initVolume() error {
	h.Volume = new(gfapi.Volume)

	if ret := h.Init(h.VolHost, h.VolName); ret != 0 {
		return fmt.Errorf("init Volume %s on %s error: %d\n", h.VolName, h.VolHost, ret)
	}

	if ret, _ := h.SetLogging(h.VolLog, gfapi.LogInfo); ret != 0 {
		return fmt.Errorf("set log to %s error: %d\n", h.VolLog, ret)
	}

	if ret := h.Mount(); ret != 0 {
		return fmt.Errorf("mount %s error: %d\n", h.VolName, ret)
	}

	return nil
}

// Close releases resources.
func (h *GlusterHandler) Close() error {
	h.Unmount()
	return nil // For compatible with Unmount returns.
}

// Create creates a DFSFile for write.
func (h *GlusterHandler) Create(info *transfer.FileInfo) (DFSFile, error) {
	gridFile, err := h.gridfs.Create(info.Name)
	if err != nil {
		return nil, err
	}

	oid, ok := gridFile.Id().(bson.ObjectId)
	if !ok {
		return nil, fmt.Errorf("id %v is not an ObjectId", gridFile.Id())
	}

	// For compatible with dfs 1.0.
	// This is a bug of driver in go, chunk size in java driver is 256k,
	// but in go is 255k. So we must reset it to 256k.
	gridFile.SetChunkSize(256 * 1024)

	filePath := util.GetFilePath(h.VolBase, info.Domain, oid.Hex(), h.PathVersion, h.PathDigit)
	dir := filepath.Dir(filePath)
	if err := h.Volume.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	file, err := h.createGlusterFile(filePath)
	if err != nil {
		return nil, err
	}
	file.grf = gridFile

	// Make a copy of file info to hold information of file.
	inf := *info
	inf.Id = oid.Hex()
	inf.Size = 0
	file.info = &inf

	return file, nil
}

func (h *GlusterHandler) createGlusterFile(name string) (*GlusterFile, error) {
	f, err := h.Volume.Create(name)
	if err != nil {
		return nil, err
	}

	return &GlusterFile{
		glf:     f,
		md5:     md5.New(),
		mode:    FileModeWrite,
		handler: h,
	}, nil
}

// Open opens a file for read.
func (h *GlusterHandler) Open(id string, domain int64) (DFSFile, error) {
	if !bson.IsObjectIdHex(id) {
		return nil, fmt.Errorf("file id is not an ObjectId: %v", id)
	}
	gridFile, err := h.gridfs.OpenId(bson.ObjectIdHex(id))
	if err != nil {
		return nil, err
	}

	filePath := util.GetFilePath(h.VolBase, domain, id, h.PathVersion, h.PathDigit)
	result, err := h.openGlusterFile(filePath)
	if err != nil {
		return nil, err
	}

	result.grf = gridFile
	result.info = &transfer.FileInfo{
		Id:     id,
		Domain: domain,
		Name:   gridFile.Name(),
		Size:   gridFile.Size(),
		Md5:    gridFile.MD5(),
	}

	return result, nil
}

// Remove deletes file by its id and domain.
func (h *GlusterHandler) Remove(id string, domain int64) error {
	if !bson.IsObjectIdHex(id) {
		return fmt.Errorf("file id is not an ObjectId: %v", id)
	}

	// TODO:(hanyh) log the remove event.
	filePath := util.GetFilePath(h.VolBase, domain, id, h.PathVersion, h.PathDigit)
	if err := h.Unlink(filePath); err != nil {
		return err
	}

	return h.gridfs.RemoveId(bson.ObjectIdHex(id))
}

func (h *GlusterHandler) openGlusterFile(name string) (*GlusterFile, error) {
	f, err := h.Volume.Open(name)
	if err != nil {
		return nil, err
	}

	return &GlusterFile{
		glf:     f,
		mode:    FileModeRead,
		handler: h,
	}, nil
}

// NewGlusterHandler creates a GlusterHandler.
func NewGlusterHandler(shardInfo *metadata.Shard, volLog string) (*GlusterHandler, error) {
	handler := &GlusterHandler{
		Shard:  shardInfo,
		VolLog: volLog,
	}

	if err := handler.initVolume(); err != nil {
		return nil, err
	}

	session, err := metadata.OpenMongoSession(shardInfo.Uri)
	if err != nil {
		return nil, err
	}

	handler.session = session
	handler.gridfs = session.Copy().DB(shardInfo.Name).GridFS("fs")

	return handler, nil
}

// GlusterFile implements DFSFile
type GlusterFile struct {
	info    *transfer.FileInfo
	glf     *gfapi.File   // Gluster file
	grf     *mgo.GridFile // GridFile
	md5     hash.Hash
	mode    dfsFileMode
	handler *GlusterHandler
}

// GetFileInfo returns file meta info.
func (f GlusterFile) GetFileInfo() *transfer.FileInfo {
	return f.info
}

// Read reads atmost len(p) bytes into p.
// Returns number of bytes read and an error if any.
func (f GlusterFile) Read(p []byte) (int, error) {
	return f.glf.Read(p)
}

// Write writes len(p) bytes to the file.
// Returns number of bytes written and an error if any.
func (f GlusterFile) Write(p []byte) (int, error) {
	l, err := f.glf.Write(p)
	if err != nil {
		return 0, err
	}

	f.md5.Write(p)
	f.info.Size += int64(l)

	return l, nil
}

// Close closes an open GlusterFile.
// Returns an error on failure.
func (f GlusterFile) Close() error {
	if err := f.glf.Close(); err != nil {
		return err
	}

	if err := f.grf.Close(); err != nil {
		return err
	}

	if f.mode == FileModeWrite {
		return f.updateMetadata()
	}

	return nil
}

func (f GlusterFile) updateMetadata() error {
	var opdata bson.D

	opdata = append(opdata, bson.DocElem{
		"domain", f.info.Domain,
	})
	opdata = append(opdata, bson.DocElem{
		"length", f.info.Size,
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
	opdata = append(opdata, bson.DocElem{
		"md5", hex.EncodeToString(f.md5.Sum(nil)),
	})

	return f.handler.gridfs.Files.Update(
		bson.M{
			"_id": f.grf.Id(),
		},
		bson.M{
			"$set": opdata,
		},
	)
}
