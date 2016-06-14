package fileop

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/kshlm/gogfapi/gfapi"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// GlusterHandler implements DFSFileHandler.
type GlusterHandler struct {
	*metadata.Shard

	session *mgo.Session
	gridfs  *mgo.GridFS
	duplfs  *DuplFs

	*gfapi.Volume
	VolLog string // Log file name of gluster volume
}

// Name returns handler's name.
func (h *GlusterHandler) Name() string {
	return h.Shard.Name
}

// initVolume initializes gluster volume.
func (h *GlusterHandler) initVolume() error {
	h.Volume = new(gfapi.Volume)

	if ret := h.Init(h.VolHost, h.VolName); ret != 0 {
		return fmt.Errorf("init volume %s on %s error: %d\n", h.VolName, h.VolHost, ret)
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

func (h *GlusterHandler) copySessionAndGridFS() (*mgo.Session, *mgo.GridFS) {
	session, err := metadata.CopySession(h.Uri)
	if err != nil {
		log.Printf("Error, session is nil")
	}

	return session, session.DB(h.Shard.Name).GridFS("fs")
}

// releaseSession releases a session if err occured.
func (h *GlusterHandler) releaseSession(session *mgo.Session, err error) {
	if err != nil && session != nil {
		metadata.ReleaseSession(session)
	}
}

// Create creates a DFSFile for write.
func (h *GlusterHandler) Create(info *transfer.FileInfo) (f DFSFile, err error) {
	session, gridfs := h.copySessionAndGridFS()
	defer h.releaseSession(session, err)

	gridFile, er := gridfs.Create(info.Name)
	if er != nil {
		err = er
		return
	}

	oid, ok := gridFile.Id().(bson.ObjectId)
	if !ok {
		err = fmt.Errorf("Invalid id, %T, %v", gridFile.Id(), gridFile.Id())
		return
	}

	// For compatible with dfs 1.0.
	// This is a bug of driver in go, chunk size in java driver is 256k,
	// but in go is 255k. So we must reset it to 256k.
	gridFile.SetChunkSize(256 * 1024)

	filePath := util.GetFilePath(h.VolBase, info.Domain, oid.Hex(), h.PathVersion, h.PathDigit)
	dir := filepath.Dir(filePath)
	if err = h.Volume.MkdirAll(dir, 0755); err != nil && !os.IsExist(err) {
		return
	}

	var file *GlusterFile
	file, err = h.createGlusterFile(filePath)
	if err != nil {
		return nil, err
	}
	file.grf = gridFile
	file.session = session
	file.gridfs = gridfs

	// Make a copy of file info to hold information of file.
	inf := *info
	inf.Id = oid.Hex()
	inf.Size = 0
	file.info = &inf

	f = file

	return
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
func (h *GlusterHandler) Open(id string, domain int64) (f DFSFile, err error) {
	session, gridfs := h.copySessionAndGridFS()
	defer h.releaseSession(session, err)

	gridFile, er := h.duplfs.Find(gridfs, id)
	if er != nil {
		err = er
		return
	}

	filePath := util.GetFilePath(h.VolBase, domain, id, h.PathVersion, h.PathDigit)
	result, er := h.openGlusterFile(filePath)
	if er != nil {
		err = er
		return
	}

	result.grf = gridFile
	result.session = session
	result.gridfs = gridfs
	result.info = &transfer.FileInfo{
		Id:     id,
		Domain: domain,
		Name:   gridFile.Name(),
		Size:   gridFile.Size(),
		Md5:    gridFile.MD5(),
	}
	f = result

	return
}

// Duplicate duplicates an entry for a file.
func (h *GlusterHandler) Duplicate(oid string) (string, error) {
	return h.duplfs.Duplicate(h.gridfs, oid)
}

// Find finds a file, if the file not exists, return empty string.
// If the file exists, return its file id.
// If the file exists and is a duplication, return its primitive file id.
func (h *GlusterHandler) Find(id string) (string, error) {
	gridFile, err := h.duplfs.Find(h.gridfs, id)
	if err == mgo.ErrNotFound {
		return "", nil
	}
	if err != nil {
		log.Printf("Failed to find file %s", id)
		return "", err
	}
	defer gridFile.Close()

	oid, ok := gridFile.Id().(bson.ObjectId)
	if !ok {
		log.Printf("Failed to find file %s", id)
		return "", fmt.Errorf("find file error %s", id)
	}

	log.Printf("Succeeded to find file %s, return %s", id, oid.Hex())

	return oid.Hex(), nil
}

// Remove deletes file by its id and domain.
func (h *GlusterHandler) Remove(id string, domain int64) (bool, *FileMeta, error) {
	f, err := h.duplfs.Find(h.gridfs, id)
	if err != nil {
		return false, nil, err
	}
	defer f.Close()

	query := bson.D{
		{"_id", f.Id()},
	}
	m, err := LookupFileMeta(h.gridfs, query)
	if err != nil {
		return false, nil, err
	}

	result, err := h.duplfs.Delete(h.gridfs, id)
	if err != nil {
		log.Printf("Failed to remove file: %s, error: %v", id, err)
		return false, nil, err
	}

	if result {
		filePath := util.GetFilePath(h.VolBase, domain, id, h.PathVersion, h.PathDigit)
		if err := h.Unlink(filePath); err != nil {
			return result, nil, err
		}
	}

	return result, m, nil
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

// HandlerType returns type of the handler.
func (h *GlusterHandler) HandlerType() HandlerType {
	return GlusterType
}

// IsHealthy checks whether shard is ok.
func (h *GlusterHandler) IsHealthy() bool {
	if err := h.session.Ping(); err != nil {
		return false
	}

	magicDirPath := filepath.Join(h.VolBase, "health", transfer.NodeName)
	if err := h.Volume.MkdirAll(magicDirPath, 0755); err != nil {
		log.Printf("IsHealthy error: %v", err)
		return false
	}

	fn := strconv.Itoa(int(time.Now().Unix()))
	magicFilePath := filepath.Join(magicDirPath, fn)
	if _, err := h.Volume.Create(magicFilePath); err != nil {
		log.Printf("IsHealthy error: %v", err)
		return false
	}
	if err := h.Volume.Unlink(magicFilePath); err != nil {
		log.Printf("IsHealthy error: %v", err)
		return false
	}

	return true
}

// FindByMd5 finds a file by its md5.
func (h *GlusterHandler) FindByMd5(md5 string, domain int64, size int64) (string, error) {
	file, err := h.duplfs.FindByMd5(h.gridfs, md5, domain, size)
	if err != nil {
		return "", err
	}

	oid, ok := file.Id().(bson.ObjectId)
	if !ok {
		return "", fmt.Errorf("Invalid id, %T, %v", file.Id(), file.Id())
	}

	return oid.Hex(), nil
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

	session, err := metadata.CopySession(shardInfo.Uri)
	if err != nil {
		return nil, err
	}

	handler.session = session
	handler.gridfs = session.DB(handler.Shard.Name).GridFS("fs")

	duplOp, err := metadata.NewDuplicateOp(shardInfo.Name, shardInfo.Uri, "fs")
	if err != nil {
		return nil, err
	}

	handler.duplfs = NewDuplFs(duplOp)

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

	session *mgo.Session
	gridfs  *mgo.GridFS
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
	defer func() {
		f.gridfs = nil
		if f.session != nil {
			metadata.ReleaseSession(f.session)
		}
	}()

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
		"bizname", f.info.Biz, // For compatible with dfs 1.0
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

	return f.gridfs.Files.Update(
		bson.M{
			"_id": f.grf.Id(),
		},
		bson.M{
			"$set": opdata,
		},
	)
}