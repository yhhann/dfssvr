package fileop

import (
	"log"
	"time"

	"jingoal/dfs/recovery"
	"jingoal/dfs/transfer"
)

type DegradeHandler struct {
	fh   DFSFileHandler
	reOp *recovery.RecoveryEventOp
}

// Name returns handler's name.
func (h *DegradeHandler) Name() string {
	return h.fh.Name()
}

// Create creates a DFSFile for write
func (h *DegradeHandler) Create(info *transfer.FileInfo) (DFSFile, error) {
	f, err := h.fh.Create(info)
	if err != nil {
		return nil, err
	}

	// Save the degradation event for recovery.
	re := recovery.RecoveryEvent{
		Domain:    info.Domain,
		Fid:       info.Id,
		Timestamp: time.Now().Unix(),
	}

	err = h.reOp.SaveEvent(&re)
	if err != nil { // Log and ignore the event saving error.
		log.Printf("DEGRADE log error, log[%s], error[%v]", re.String(), err)
	}
	return f, nil
}

// Open opens a DFSFile for read
func (h *DegradeHandler) Open(id string, domain int64) (DFSFile, error) {
	return h.fh.Open(id, domain)
}

// Duplicate duplicates an entry for a file.
func (h *DegradeHandler) Duplicate(oid string) (string, error) {
	return h.fh.Duplicate(oid)
}

// Find finds a file, if the file not exists, return empty string.
// If the file exists, return its file id.
// If the file exists and is a duplication, return its primitive file id.
func (h *DegradeHandler) Find(fid string) (string, error) {
	return h.fh.Find(fid)
}

// Remove deletes a file with its id and domain.
func (h *DegradeHandler) Remove(id string, domain int64) (bool, error) {
	return h.fh.Remove(id, domain)
}

// Close releases resources the handler holds.
func (h *DegradeHandler) Close() error {
	return h.fh.Close()
}

// HandlerType returns type of the handler.
func (h *DegradeHandler) HandlerType() HandlerType {
	return DegradeType
}

// IsHealthy checks whether shard is ok.
func (h *DegradeHandler) IsHealthy() bool {
	return h.fh.IsHealthy()
}

// FindByMd5 finds a file by its md5.
func (h *DegradeHandler) FindByMd5(md5 string, domain int64, size int64) (string, error) {
	return h.FindByMd5(md5, domain, size)
}

// NewDegradeHandler returns a handler for processing Degraded files.
func NewDegradeHandler(handler DFSFileHandler, reop *recovery.RecoveryEventOp) *DegradeHandler {
	return &DegradeHandler{
		fh:   handler,
		reOp: reop,
	}
}
