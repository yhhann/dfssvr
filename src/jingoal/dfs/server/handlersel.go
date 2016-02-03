package server

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"

	"jingoal/dfs/fileop"
	"jingoal/dfs/metadata"
)

type handlerStatus uint

const (
	statusOk      handlerStatus = iota // handler for shard is ok.
	statusFailure                      // handler for shard is failure.
)

// HandlerSelector selects a perfect file handler for dfs server.
type HandlerSelector struct {
	segments       []*metadata.Segment
	handlers       map[string]fileop.DFSFileHandler
	degradeHandler fileop.DFSFileHandler
	status         map[fileop.DFSFileHandler]handlerStatus

	rwLock sync.RWMutex
}

// updateHandler creates or updates a handler for given shard.
func (hs *HandlerSelector) updateHandler(shard *metadata.Shard) {
	var handler fileop.DFSFileHandler
	var err error

	status := statusOk
	defer func() {
		hs.status[handler] = status
	}()

	if len(shard.VolHost) != 0 && len(shard.VolName) != 0 { // GlusterFS
		handler, err = fileop.NewGlusterHandler(shard, filepath.Join(*logDir, shard.Name))
	} else { // GridFS
		handler, err = fileop.NewGridFsHandler(shard)
	}

	if err != nil {
		log.Printf("Failed to create handler, shard: %+v, error: %v", shard, err)
		status = statusFailure
		return
	}

	if shard.ShdType == metadata.DegradeServer {
		hs.degradeHandler = handler
		log.Printf("Succeeded to create degrade handler, shard: %+v", shard)
		return
	}

	if h, ok := hs.handlers[shard.Name]; ok {
		h.Close()
	}

	hs.handlers[shard.Name] = handler

	log.Printf("Succeeded to create handler, shard: %+v", shard)
}

// getDfsFileHandler returns perfect file handlers to process file.
// The first returned handler is for normal handler,
// and the second one is for file migrating.
func (hs *HandlerSelector) getDFSFileHandler(domain int64) (*fileop.DFSFileHandler, *fileop.DFSFileHandler, error) {
	if len(hs.handlers) == 0 {
		return nil, nil, fmt.Errorf("no handler")
	}

	seg := FindPerfectSegment(hs.segments, domain)
	if seg == nil {
		return nil, nil, fmt.Errorf("Failed to find perfect server, domain %d", domain)
	}

	h, ok := hs.handlers[seg.NormalServer]
	if !ok {
		return nil, nil, fmt.Errorf("no normal site, seg: %+v", seg)
	}

	m, ok := hs.handlers[seg.MigrateServer]
	if ok {
		return &h, &m, nil
	}
	return &h, nil, nil
}

// checkOrDegrade checks status of given handler,
// if status is offline, degrade.
func (hs *HandlerSelector) checkOrDegrade(handler *fileop.DFSFileHandler) (*fileop.DFSFileHandler, error) {
	if handler == nil { // Check for nil.
		return nil, fmt.Errorf("Failed to degrade: handler is nil")
	}

	if status, ok := hs.getStatus(*handler); ok && status == statusOk {
		return handler, nil
	}

	if status, ok := hs.getStatus(hs.degradeHandler); ok && status == statusOk {
		return &hs.degradeHandler, nil
	}

	return nil, fmt.Errorf("Failed to degrade: %v", (*handler).Name())
}

// getDfsFileHandlerForWrite returns perfect file handlers to write file.
func (hs *HandlerSelector) getDFSFileHandlerForWrite(domain int64) (*fileop.DFSFileHandler, error) {
	nh, mh, err := hs.getDFSFileHandler(domain)
	if err != nil {
		return nil, err
	}

	handler := nh
	if mh != nil {
		handler = mh
	}

	return hs.checkOrDegrade(handler)
}

// getDfsFileHandlerForRead returns perfect file handlers to read file.
func (hs *HandlerSelector) getDFSFileHandlerForRead(domain int64) (*fileop.DFSFileHandler, *fileop.DFSFileHandler, error) {
	n, m, err := hs.getDFSFileHandler(domain)
	if err != nil {
		return nil, nil, err
	}

	m, _ = hs.checkOrDegrade(m) // Need not check this error.
	n, err = hs.checkOrDegrade(n)
	// Need not return err, for we will verify the pair of n and m
	// outside this function.
	if err != nil {
		log.Printf("%v", err)
	}

	return n, m, nil
}

func (hs *HandlerSelector) updateStatus(name string, status handlerStatus) {
	hs.rwLock.Lock()
	defer hs.rwLock.Unlock()

	if h, ok := hs.handlers[name]; ok {
		hs.status[h] = status
	}
}

func (hs *HandlerSelector) getStatus(h fileop.DFSFileHandler) (handlerStatus, bool) {
	hs.rwLock.RLock()
	defer hs.rwLock.RUnlock()

	status, ok := hs.status[h]
	return status, ok
}

func NewHandlerSelector(segments []*metadata.Segment, shards []*metadata.Shard) (*HandlerSelector, error) {
	selector := new(HandlerSelector)
	selector.handlers = make(map[string]fileop.DFSFileHandler)
	selector.status = make(map[fileop.DFSFileHandler]handlerStatus)
	selector.segments = segments

	for _, shard := range shards {
		selector.updateHandler(shard)
	}

	return selector, nil
}

// FindPerfectSegment finds a perfect segment for domain.
// Segments must be in ascending order.
func FindPerfectSegment(segments []*metadata.Segment, domain int64) *metadata.Segment {
	var result *metadata.Segment

	for _, seg := range segments {
		if domain > seg.Domain {
			result = seg
			continue
		} else if domain == seg.Domain {
			result = seg
			break
		} else {
			break
		}
	}

	return result
}
