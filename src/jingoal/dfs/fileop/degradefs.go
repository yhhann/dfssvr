package fileop

import "jingoal/dfs/transfer"

type DegradeHandler struct {
	//fh *GridFsHandler
	fh DFSFileHandler
}

// Name returns handler's name.
func (h *DegradeHandler) Name() string {
	return h.fh.Name()
}

// Create creates a DFSFile for write
func (h *DegradeHandler) Create(info *transfer.FileInfo) (DFSFile, error) {
	return h.fh.Create(info)
}

// Open opens a DFSFile for read
func (h *DegradeHandler) Open(id string, domain int64) (DFSFile, error) {
	return h.fh.Open(id, domain)
}

// Remove deletes a file with its id and domain.
func (h *DegradeHandler) Remove(id string, domain int64) error {
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

// NewDegradeHandler returns a handler for processing Degraded files.
func NewDegradeHandler(handler DFSFileHandler) *DegradeHandler {
	return &DegradeHandler{
		fh: handler,
	}
}
