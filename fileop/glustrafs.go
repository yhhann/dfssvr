package fileop

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/glog"
	"github.com/kshlm/gogfapi/gfapi"
	"gopkg.in/mgo.v2/bson"

	dra "jingoal.com/dfs/cassandra"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

const (
	GlustraAttrUser        = "user"        // string
	GlustraAttrPass        = "pass"        // string
	GlustraAttrKeyspace    = "keyspace"    // string
	GlustraAttrConsistency = "consistency" // string
	GlustraAttrPort        = "port"        // integer
	GlustraAttrTimeout     = "timeout"     // integer
	GlustraAttrConns       = "conns"       // integer
)

const (
	GlustraDefaultChunkSize = 512 * 1024
)

// GlustraHandler implements DFSFileHandler.
type GlustraHandler struct {
	*metadata.Shard
	*gfapi.Volume

	draOp  *dra.MetaOp
	duplfs *DuplDra

	VolLog string // Log file name of gluster volume
}

// Name returns handler's name.
func (h *GlustraHandler) Name() string {
	return h.Shard.Name
}

func (h *GlustraHandler) initLogDir() error {
	logDir := filepath.Dir(h.VolLog)

	_, err := os.Stat(logDir)
	if os.IsNotExist(err) {
		return os.MkdirAll(logDir, 0700)
	}
	if err != nil {
		return err
	}

	return nil
}

// initVolume initializes gluster volume.
func (h *GlustraHandler) initVolume() error {
	h.Volume = new(gfapi.Volume)

	if ret := h.Init(h.VolHost, h.VolName); ret != 0 {
		return fmt.Errorf("init volume %s on %s error: %d\n", h.VolName, h.VolHost, ret)
	}

	if err := h.initLogDir(); err != nil {
		return fmt.Errorf("Failed to create log directory: %v", err)
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
func (h *GlustraHandler) Close() error {
	h.Unmount()
	return nil // For compatible with Unmount returns.
}

// Create creates a DFSFile for write.
func (h *GlustraHandler) Create(info *transfer.FileInfo) (DFSFile, error) {
	oid := bson.NewObjectId()

	filePath := util.GetFilePath(h.VolBase, info.Domain, oid.Hex(), h.PathVersion, h.PathDigit)
	dir := filepath.Dir(filePath)
	if err := h.Volume.MkdirAll(dir, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}

	file, err := h.createGlustraFile(filePath)
	if err != nil {
		return nil, err
	}

	file.sdf = &dra.File{
		Id:        oid.Hex(),
		Domain:    info.Domain,
		Biz:       info.Biz,
		Name:      info.Name,
		UserId:    fmt.Sprintf("%d", info.User),
		ChunkSize: -1, // means no use.
		Type:      dra.EntitySeadraFS,
		Metadata:  make(map[string]string),
	}

	// Make a copy of file info to hold information of file.
	inf := *info
	inf.Id = file.sdf.Id
	inf.Size = file.sdf.Size
	file.info = &inf

	return file, nil
}

func (h *GlustraHandler) createGlustraFile(name string) (*GlustraFile, error) {
	f, err := h.Volume.Create(name)
	if err != nil {
		return nil, err
	}

	return &GlustraFile{
		glf:     f,
		md5:     md5.New(),
		mode:    FileModeWrite,
		handler: h,
	}, nil
}

// Open opens a file for read.
func (h *GlustraHandler) Open(id string, domain int64) (DFSFile, error) {
	f, err := h.duplfs.Find(id)
	if err != nil {
		return nil, err
	}

	filePath := util.GetFilePath(h.VolBase, f.Domain, f.Id, h.PathVersion, h.PathDigit)
	result, err := h.openGlustraFile(filePath)
	if err != nil {
		return nil, err
	}

	result.sdf = f
	result.info = &transfer.FileInfo{
		Id:     f.Id,
		Domain: f.Domain,
		Name:   f.Name,
		Size:   f.Size,
		Md5:    f.Md5,
		Biz:    f.Biz,
	}
	return result, nil
}

// Duplicate duplicates an entry for a file.
func (h *GlustraHandler) Duplicate(fid string) (string, error) {
	return h.duplfs.Duplicate(fid)
}

// Find finds a file. If the file not exists, return empty string.
// If the file exists and is a duplication, return its primitive file ID.
// If the file exists, return its file ID.
func (h *GlustraHandler) Find(id string) (string, *DFSFileMeta, *transfer.FileInfo, error) {
	f, err := h.duplfs.Find(id)
	if err != nil {
		return "", nil, nil, err
	}

	var chunksize int64 = 0
	chunksize, err = strconv.ParseInt(f.Metadata["chunksize"], 10, 64)
	if err != nil {
		glog.V(4).Infof("Failed to parse chunk size %s, %v.", f.Metadata["chunksize"], err)
	}

	meta := &DFSFileMeta{
		Bizname:   f.Biz,
		Fid:       f.Metadata[BSMetaKey_WeedFid],
		ChunkSize: chunksize,
	}

	info := &transfer.FileInfo{
		Id:     id,
		Name:   f.Name,
		Size:   f.Size,
		Md5:    f.Md5,
		Biz:    f.Biz,
		Domain: f.Domain,
		// TODO(hanyh): add user id
	}

	glog.V(3).Infof("Succeeded to find file %s, return %s", id, f.Id)

	return f.Id, meta, info, nil
}

// Remove deletes file by its id and domain.
func (h *GlustraHandler) Remove(id string, domain int64) (bool, *FileMeta, error) {
	result, entityId, err := h.duplfs.LazyDelete(id)
	if err != nil {
		glog.Warningf("Failed to remove file %s %d, error: %s", id, domain, err)
		return false, nil, err
	}

	var m *FileMeta
	if result {
		m, err = h.duplfs.LookupFileMeta(entityId)
		if err != nil {
			return false, nil, err
		}
		h.draOp.RemoveFile(entityId)

		filePath := util.GetFilePath(h.VolBase, domain, entityId, h.PathVersion, h.PathDigit)
		if err := h.Unlink(filePath); err != nil {
			glog.Warningf("Failed to remove file %s %d from %s", id, domain, filePath)
		}
	}

	return result, m, nil
}

func (h *GlustraHandler) openGlustraFile(name string) (*GlustraFile, error) {
	f, err := h.Volume.Open(name)
	if err != nil {
		return nil, err
	}

	return &GlustraFile{
		glf:     f,
		mode:    FileModeRead,
		handler: h,
	}, nil
}

// IsHealthy checks whether shard is ok.
func (h *GlustraHandler) IsHealthy() bool {
	return h.HealthStatus() == HealthOk
}

// HealthStatus returns the status of node health.
func (h *GlustraHandler) HealthStatus() int {
	// TODO(hanyh): check cassandra?

	magicDirPath := filepath.Join(h.VolBase, "health", transfer.ServerId)
	if err := h.Volume.MkdirAll(magicDirPath, 0755); err != nil {
		glog.Warningf("IsHealthy error: %v", err)
		return StoreNotHealthy
	}

	fn := strconv.Itoa(int(time.Now().Unix()))
	magicFilePath := filepath.Join(magicDirPath, fn)
	if _, err := h.Volume.Create(magicFilePath); err != nil {
		glog.Warningf("IsHealthy error: %v", err)
		return StoreNotHealthy
	}
	if err := h.Volume.Unlink(magicFilePath); err != nil {
		glog.Warningf("IsHealthy error: %v", err)
		return StoreNotHealthy
	}

	return HealthOk
}

// FindByMd5 finds a file by its md5.
func (h *GlustraHandler) FindByMd5(md5 string, domain int64, size int64) (string, error) {
	file, err := h.duplfs.FindByMd5(md5, domain) // ignore size
	if err != nil {
		return "", err
	}

	return file.Id, nil
}

// NewGlustraHandler creates a GlustraHandler.
func NewGlustraHandler(si *metadata.Shard, volLog string) (*GlustraHandler, error) {
	handler := &GlustraHandler{
		Shard:  si,
		VolLog: volLog,
	}

	if err := handler.initVolume(); err != nil {
		return nil, err
	}

	if si.ShdType != metadata.Glustra {
		return nil, fmt.Errorf("invalid shard type %d.", si.ShdType)
	}

	seeds := strings.Split(si.Uri, ",")
	handler.draOp = dra.NewMetaOp(seeds, parseCqlOptions(si.Attr)...)

	handler.duplfs = NewDuplDra(handler.draOp)

	return handler, nil
}

// parseCqlOptions parses options of cassandra from map.
func parseCqlOptions(attr map[string]interface{}) []func(*dra.MetaOp) {
	options := make([]func(*dra.MetaOp), 0, len(attr))

	options = append(options, func(m *dra.MetaOp) {
		port, ok := attr[GlustraAttrPort].(int)
		if !ok {
			return
		}
		m.Port = port
	})

	options = append(options, func(m *dra.MetaOp) {
		timeout, ok := attr[GlustraAttrTimeout].(int)
		if !ok {
			return
		}
		m.Timeout = time.Millisecond * time.Duration(timeout)
	})

	options = append(options, func(m *dra.MetaOp) {
		conns, ok := attr[GlustraAttrConns].(int)
		if !ok {
			return
		}
		m.NumConns = conns
	})

	options = append(options, func(m *dra.MetaOp) {
		ks := attr[GlustraAttrKeyspace].(string) // panic
		m.Keyspace = ks
	})

	options = append(options, func(m *dra.MetaOp) {
		consistency, ok := attr[GlustraAttrConsistency].(string)
		if !ok {
			return
		}
		m.Consistency = gocql.ParseConsistency(consistency)
	})

	options = append(options, func(m *dra.MetaOp) {
		user, ok := attr[GlustraAttrUser].(string)
		if !ok || len(user) == 0 {
			return
		}
		pass, ok := attr[GlustraAttrPass].(string)
		if !ok {
			return
		}

		m.Authenticator = gocql.PasswordAuthenticator{
			Username: user,
			Password: pass,
		}
	})

	return options
}

// GlustraFile implements DFSFile
type GlustraFile struct {
	info    *transfer.FileInfo
	glf     *gfapi.File // Gluster file
	sdf     *dra.File
	md5     hash.Hash
	mode    dfsFileMode
	handler *GlustraHandler
}

// GetFileInfo returns file meta info.
func (f GlustraFile) GetFileInfo() *transfer.FileInfo {
	return f.info
}

// Read reads atmost len(p) bytes into p.
// Returns number of bytes read and an error if any.
func (f GlustraFile) Read(p []byte) (int, error) {
	nr, er := f.glf.Read(p)
	// When reached EOF, glf returns nr=0 other than er=io.EOF, fix it.
	if nr <= 0 {
		return 0, io.EOF
	}
	return nr, er
}

// Write writes len(p) bytes to the file.
// Returns number of bytes written and an error if any.
func (f GlustraFile) Write(p []byte) (int, error) {
	// If len(p) is zero, glf.Write() will panic.
	if len(p) == 0 { // fix bug of gfapi.
		return 0, nil
	}

	n, err := f.glf.Write(p)
	if err != nil {
		return 0, err
	}

	f.md5.Write(p)
	f.info.Size += int64(n)
	f.sdf.Size += int64(n)

	return n, nil
}

// Close closes an opened GlustraFile.
func (f GlustraFile) Close() error {
	if err := f.glf.Close(); err != nil {
		return err
	}

	if f.mode == FileModeWrite {
		f.sdf.UploadDate = time.Now()
		f.sdf.Md5 = hex.EncodeToString(f.md5.Sum(nil))
		if err := f.handler.draOp.SaveFile(f.sdf); err != nil {
			h := f.handler
			inf := f.info
			filePath := util.GetFilePath(h.VolBase, inf.Domain, inf.Id, h.PathVersion, h.PathDigit)
			if err := h.Unlink(filePath); err != nil {
				glog.Warningf("Failed to remove file without meta, %s %s %d from %s", inf.Id, inf.Name, inf.Domain, filePath)
			}

			return err
		}
	}

	return nil
}

// updateFileMeta updates file dfs meta.
func (f GlustraFile) updateFileMeta(m map[string]interface{}) {
	f.sdf.Metadata = make(map[string]string)
	for k, v := range m {
		f.sdf.Metadata[k] = toString(v)
	}
}

func toString(x interface{}) string {
	switch x := x.(type) {
	case nil:
		return "NULL"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", x)
	case float32, float64:
		return fmt.Sprintf("%g", x)
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case string:
		return x
	default:
		glog.Warningf("unexpected type %T: %v", x, x)
		return ""
	}
}

// getFileMeta returns file dfs meta.
func (f GlustraFile) getFileMeta() *DFSFileMeta {
	ck, err := strconv.ParseInt(f.sdf.Metadata[BSMetaKey_Chunksize], 10, 64)
	if err != nil {
		glog.Warningf("Failed to parse chunk size, %s", f.sdf.Metadata[BSMetaKey_Chunksize])
		ck = GlustraDefaultChunkSize
	}
	return &DFSFileMeta{
		Bizname:   f.sdf.Biz,
		Fid:       f.sdf.Metadata[BSMetaKey_WeedFid],
		ChunkSize: ck,
	}
}
