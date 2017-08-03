package fileop

import (
	"flag"
	"strings"

	"github.com/golang/glog"

	"jingoal.com/dfs/conf"
	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/seaweedfs-adaptor/weedfs"
)

const (
	BSMetaKey_WeedFid   = "weedfid"
	BSMetaKey_Chunksize = "chunksize"
)

var (
	bsWithRealName = flag.Bool("bs-with-real-name", false, "create backstore file with the real name.")

	NegotiatedChunkSize = int64(1048576)
)

// BackStoreHandler implements interface DFSFileHandler.
type BackStoreHandler struct {
	// embedded DFSFileHandler
	DFSFileHandler

	// shard for back store
	BackStoreShard *metadata.Shard
}

// Create creates a DFSFile for write
func (bsh *BackStoreHandler) Create(info *transfer.FileInfo) (DFSFile, error) {
	originalFile, err := bsh.DFSFileHandler.Create(info)
	if err != nil {
		return nil, err
	}

	var wFile *weedfs.WeedFile
	if isWriteToBackStore(info.Domain) {
		fn := ""
		if *bsWithRealName {
			fn = info.Name
			glog.V(5).Infof("Create backstore file with real name %s.", fn)
		}
		wFile, err = weedfs.Create(fn, info.Domain, bsh.BackStoreShard.MasterUri, bsh.BackStoreShard.Replica, bsh.BackStoreShard.DataCenter, bsh.BackStoreShard.Rack, NegotiatedChunkSize)
		if err != nil {
			instrument.BackstoreFileCounter <- &instrument.Measurements{
				Name:  "create_failed",
				Value: 1.0,
			}
			glog.Warningf("Failed to create file %v", err)
			return NewBackStoreFile(nil, originalFile, info), nil
		}

		originalFile.updateFileMeta(map[string]interface{}{
			BSMetaKey_WeedFid:   wFile.Fid,
			BSMetaKey_Chunksize: wFile.GetChunkSize(),
		})
		instrument.BackstoreFileCounter <- &instrument.Measurements{
			Name:  "created",
			Value: 1.0,
		}
		glog.V(3).Infof("Succeeded to create backstore file: %s, %s", wFile.Fid, info.Name)
	}

	return NewBackStoreFile(wFile, originalFile, info), nil
}

// Open opens a DFSFile for read
func (bsh *BackStoreHandler) Open(id string, domain int64) (DFSFile, error) {
	_, meta, info, err := bsh.DFSFileHandler.Find(id)
	if err != nil {
		return nil, err
	}

	glog.V(5).Infof("Read from bs %t, meta %+v", isReadFromBackStore(domain), meta)

	var wFile *weedfs.WeedFile
	readFromOrig := true
	if isReadFromBackStore(domain) && meta != nil && len(meta.Fid) > 0 {
		readFromOrig = false
		wFile, err = weedfs.Open(meta.Fid, domain, bsh.BackStoreShard.MasterUri)
		if err != nil {
			instrument.BackstoreFileCounter <- &instrument.Measurements{
				Name:  "opene_failed",
				Value: 1.0,
			}
			glog.Warningf("Failed to open backstore file %v", err)
			readFromOrig = true
		} else {
			if meta.ChunkSize > 0 {
				wFile.SetChunkSize(meta.ChunkSize)
			}
			instrument.BackstoreFileCounter <- &instrument.Measurements{
				Name:  "opened",
				Value: 1.0,
			}
			glog.V(3).Infof("Succeeded to open backstore file: %s, %s", id, wFile.Fid)
		}
	}

	if readFromOrig {
		originalFile, err := bsh.DFSFileHandler.Open(id, domain)
		if err != nil {
			return nil, err
		}
		glog.V(3).Infof("Succeeded to open original file: %s", id)
		return NewBackStoreFile(nil, originalFile, info), nil
	}

	return NewBackStoreFile(wFile, nil, info), nil
}

// Remove deletes a file by its id.
func (bsh *BackStoreHandler) Remove(id string, domain int64) (bool, *meta.File, error) {
	_, meta, _, err := bsh.DFSFileHandler.Find(id)
	if err != nil {
		return true, nil, nil
	}

	result, fm, err := bsh.DFSFileHandler.Remove(id, domain)
	if err != nil {
		return result, fm, err
	}
	glog.V(3).Infof("Succeeded to remove file from %s %s, %t", bsh.DFSFileHandler.Name(), id, result)

	if result {
		if meta != nil && meta.Fid != "" {
			deleteResult, err := weedfs.Remove(meta.Fid, domain, bsh.BackStoreShard.MasterUri)
			if err != nil {
				instrument.BackstoreFileCounter <- &instrument.Measurements{
					Name:  "remove_failed",
					Value: 1.0,
				}
				glog.V(3).Infof("Failed to remove file %s from %s, %v",
					meta.Fid, strings.Join([]string{bsh.Name(), bsh.BackStoreShard.Name}, "@"), err)
			} else {
				instrument.BackstoreFileCounter <- &instrument.Measurements{
					Name:  "removed",
					Value: 1.0,
				}
				glog.V(3).Infof("Succeeded to remove file %s from %s, %t",
					meta.Fid, strings.Join([]string{bsh.Name(), bsh.BackStoreShard.Name}, "@"), deleteResult)
			}
		}
	}

	return result, fm, err
}

// Close releases resources the handler holds.
func (bsh *BackStoreHandler) Close() error {
	return bsh.DFSFileHandler.Close()
}

// Duplicate duplicates an entry for a file.
func (bsh *BackStoreHandler) Duplicate(oid string) (string, error) {
	return bsh.DFSFileHandler.Duplicate(oid)
}

// Find finds a file, if the file not exists, return empty string.
// If the file exists, return its file id.
// If the file exists and is a duplication, return its primitive file id.
func (bsh *BackStoreHandler) Find(fid string) (string, *DFSFileMeta, *transfer.FileInfo, error) {
	return bsh.DFSFileHandler.Find(fid)
}

// Name returns handler's name.
func (bsh *BackStoreHandler) Name() string {
	return bsh.DFSFileHandler.Name()
}

// IsHealthy checks whether shard is ok.
func (bsh *BackStoreHandler) IsHealthy() bool {
	return bsh.DFSFileHandler.IsHealthy()
}

// HealthStatus returns the status of node health.
func (bsh *BackStoreHandler) HealthStatus() int {
	return bsh.DFSFileHandler.HealthStatus()
}

// FindByMd5 finds a file by its md5.
func (bsh *BackStoreHandler) FindByMd5(md5 string, domain int64, size int64) (string, error) {
	return bsh.DFSFileHandler.FindByMd5(md5, domain, size)
}

func NewBackStoreHandler(originalHandler DFSFileHandler, bsShard *metadata.Shard) *BackStoreHandler {
	handler := BackStoreHandler{
		DFSFileHandler: originalHandler,
		BackStoreShard: bsShard,
	}

	return &handler
}

// BackStoreFile implements DFSFile.
type BackStoreFile struct {
	// embedded DFSFile
	DFSFile

	info *transfer.FileInfo

	// back store file
	bs    *weedfs.WeedFile
	bsErr error
}

// GetFileInfo returns file info.
func (d *BackStoreFile) GetFileInfo() *transfer.FileInfo {
	if d.DFSFile != nil {
		return d.DFSFile.GetFileInfo()
	}

	return d.info
}

// Write writes a byte buffer into back store file.
func (d *BackStoreFile) Write(p []byte) (n int, err error) {
	n, err = d.DFSFile.Write(p)

	if d.bs != nil && d.bsErr == nil {
		_, d.bsErr = d.bs.Write(p)
	}
	return
}

// Read reads a byte buffer from back store file.
func (d *BackStoreFile) Read(p []byte) (n int, err error) {
	if d.bs != nil {
		n, err = d.bs.Read(p)
		if glog.V(6) {
			glog.Infof("read from weed %d, %v", n, err)
		}
		return
	}

	return d.DFSFile.Read(p)
}

// Close closes a back store file.
func (d *BackStoreFile) Close() (err error) {
	if d.bs != nil && d.bsErr == nil {
		err = d.bs.Close()
	}

	if d.DFSFile != nil {
		err = d.DFSFile.Close()
	}

	return
}

// NewBackStoreFile creates a new back store file.
func NewBackStoreFile(bs *weedfs.WeedFile, file DFSFile, info *transfer.FileInfo) *BackStoreFile {
	return &BackStoreFile{
		DFSFile: file,
		bs:      bs,
		info:    info,
	}
}

func isReadFromBackStore(domain int64) bool {
	ff, err := conf.GetFlag(conf.FlagKeyReadFromBackStore)
	if err != nil {
		glog.Warningf("feature %s error %v", conf.FlagKeyReadFromBackStore, err)
		return false
	}

	return ff.DomainHasAccess(uint32(domain))
}

func isWriteToBackStore(domain int64) bool {
	ff, err := conf.GetFlag(conf.FlagKeyBackStore)
	if err != nil {
		glog.Warningf("feature %s error %v", conf.FlagKeyBackStore, err)
		return false
	}

	return ff.DomainHasAccess(uint32(domain))
}
