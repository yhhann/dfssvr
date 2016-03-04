package server

import (
	"flag"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/mgo.v2/bson"

	"jingoal/dfs/fileop"
	"jingoal/dfs/metadata"
	"jingoal/dfs/recovery"
)

const (
	statusOk      handlerStatus = iota // handler for shard is ok.
	statusFailure                      // handler for shard is failure.
)

var (
	healthCheckInterval = flag.Int("health-check-interval", 30, "health check interval in seconds")
	healthCheckTimeout  = flag.Int("health-check-timeout", 5, "health check timeout in seconds")

	recoveryBufferSize = flag.Int("recovery-buffer-size", 100000, "size of channel for each DFSFileHandler")
	recoveryInterval   = flag.Int("recovery-interval", 60, "interval in seconds for recovery event inspection")
	recoveryBatchSize  = flag.Int("recovery-batch-size", 100000, "batch size for recovery event inspection")
)

type handlerStatus uint

func NewHandlerStatus(v bool) handlerStatus {
	if v {
		return statusOk
	}

	return statusFailure
}

func (hs handlerStatus) String() string {
	if hs == statusOk {
		return "ok"
	}

	return "failure"
}

// FileRecoveryInfo represents the information for file recovery.
type FileRecoveryInfo struct {
	Id     bson.ObjectId
	Fid    string
	Domain int64
}

func (rInfo *FileRecoveryInfo) String() string {
	return fmt.Sprintf("fid: %s, domain: %d, id: %s",
		rInfo.Fid, rInfo.Domain, rInfo.Id.String())
}

// HandlerSelector selects a perfect file handler for dfs server.
type HandlerSelector struct {
	segments       []*metadata.Segment
	handlers       map[string]fileop.DFSFileHandler
	degradeHandler fileop.DFSFileHandler
	status         map[string]handlerStatus
	reOp           *recovery.RecoveryEventOp

	// For recovery
	recoveries map[string]chan *FileRecoveryInfo

	rwLock sync.RWMutex
}

// updateHandler creates or updates a handler for given shard.
// op 1 for add a handler, 2 for delete a handler.
func (hs *HandlerSelector) updateHandler(shard *metadata.Shard, op int) {
	switch op {
	case 1:
		hs.addHandler(shard)
	case 2:
		hs.deleteHandler(shard.Name)
	}
}

func (hs *HandlerSelector) deleteHandler(handlerName string) {
	if h, ok := hs.handlers[handlerName]; ok {
		if err := h.Close(); err != nil {
			log.Printf("Failed to close the old handler: %v", h)
		}

		hs.handlers[handlerName] = nil
	}
}

func (hs *HandlerSelector) addHandler(shard *metadata.Shard) {
	var handler fileop.DFSFileHandler
	var err error

	if len(shard.VolHost) != 0 && len(shard.VolName) != 0 { // GlusterFS
		handler, err = fileop.NewGlusterHandler(shard, filepath.Join(*logDir, shard.Name))
	} else { // GridFS
		handler, err = fileop.NewGridFsHandler(shard)
	}

	if err != nil {
		log.Printf("Failed to create handler, shard: %v, error: %v", shard, err)
		return
	}

	if shard.ShdType == metadata.DegradeServer {
		if hs.degradeHandler != nil {
			log.Printf("Failed to create degrade server, since we already have one, shard: %v", shard)
			return
		}

		hs.degradeHandler = fileop.NewDegradeHandler(handler, hs.reOp)
		hs.updateHandlerStatus(handler, statusOk)
		log.Printf("Succeeded to create degrade handler, shard: %v", shard)
		return
	}

	if h, ok := hs.handlers[handler.Name()]; ok {
		if err := h.Close(); err != nil {
			log.Printf("Failed to close the old handler: %v", h)
		}
	}

	hs.handlers[handler.Name()] = handler
	hs.updateHandlerStatus(handler, statusOk)

	hs.recoveries[handler.Name()] = make(chan *FileRecoveryInfo, *recoveryBufferSize)

	log.Printf("Succeeded to create handler, shard: %v", shard)
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
		return nil, nil, fmt.Errorf("no normal site, seg: %v", seg)
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

	if status, ok := hs.getHandlerStatus(*handler); ok && status == statusOk {
		return handler, nil
	}

	if status, ok := hs.getHandlerStatus(hs.degradeHandler); ok && status == statusOk {
		log.Printf("DEGRADE!!! %v ==> %v", (*handler).Name(), hs.degradeHandler.Name())
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

func (hs *HandlerSelector) updateHandlerStatus(h fileop.DFSFileHandler, status handlerStatus) {
	hs.rwLock.Lock()
	defer hs.rwLock.Unlock()

	hs.status[h.Name()] = status
}

func (hs *HandlerSelector) getHandlerStatus(h fileop.DFSFileHandler) (handlerStatus, bool) {
	hs.rwLock.RLock()
	defer hs.rwLock.RUnlock()

	status, ok := hs.status[h.Name()]
	return status, ok
}

// startHealthyCheckRoutine starts a routine for health check.
func (hs *HandlerSelector) startHealthyCheckRoutine() {
	// Starts a routine for health check every healthCheckInterval seconds.
	go func() {
		ticker := time.NewTicker(time.Duration(*healthCheckInterval) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				for _, handler := range hs.handlers {
					// For every handler, starts a routine to check its health.
					go func(h fileop.DFSFileHandler) {
						hs.updateHandlerStatus(h, healthCheck(h))
						log.Printf("status of handler %v is %v", h.Name(), hs.status[h.Name()])
					}(handler)
				}

				// starts a routine to check degrade server.
				go func() {
					h := hs.degradeHandler
					hs.updateHandlerStatus(h, healthCheck(h))
					log.Printf("status of handler %v is %v", h.Name(), hs.status[h.Name()])
				}()
			}
		}
	}()
}

// startRecoveryRoutine starts a recovery routine for every handler.
func (hs *HandlerSelector) startRevoveryRoutine() {
	for handlerName, c := range hs.recoveries {
		h := hs.handlers[handlerName]
		go func(handler fileop.DFSFileHandler, ch chan *FileRecoveryInfo) {
			for {
				select {
				case recoveryInfo := <-ch:
					if err := copyFile(handler, hs.degradeHandler, recoveryInfo); err != nil {
						log.Printf("Failed to recovery file %s, error: %v", recoveryInfo.Fid, err)
						break
					}

					if err := hs.reOp.RemoveEvent(recoveryInfo.Id); err != nil {
						log.Printf("Failed to remove recovery event %s", recoveryInfo.String())
					}
				}
			}
		}(h, c)
	}

	go func() {
		ticker := time.NewTicker(time.Duration(*recoveryInterval) * time.Second)
		defer func() {
			ticker.Stop()
		}()

		for {
			select {
			case <-ticker.C:
				if err := hs.dispatchRecoveryEvent(*recoveryBatchSize, int64(*recoveryInterval)); err != nil {
					log.Printf("Recovery dispatch error, %v", err)
				}
			}
		}

	}()

}

// dispachRecoveryEvent dispatches recovery events.
func (hs *HandlerSelector) dispatchRecoveryEvent(batchSize int, timeout int64) error {
	events, err := hs.reOp.GetEventsInBatch(batchSize, timeout)
	if err != nil {
		return err
	}

	for _, e := range events {
		h, err := hs.getDFSFileHandlerForWrite(e.Domain)
		if err != nil {
			log.Printf("Failed to get file handler for %d", e.Domain)
			continue
		}

		hs.recoveries[(*h).Name()] <- &FileRecoveryInfo{
			Id:     e.Id,
			Fid:    e.Fid,
			Domain: e.Domain,
		}
	}

	return nil
}

func NewHandlerSelector(segments []*metadata.Segment, shards []*metadata.Shard, reop *recovery.RecoveryEventOp) (*HandlerSelector, error) {
	selector := new(HandlerSelector)
	selector.handlers = make(map[string]fileop.DFSFileHandler)
	selector.status = make(map[string]handlerStatus)
	selector.segments = segments
	selector.reOp = reop

	for _, shard := range shards {
		selector.updateHandler(shard, 1 /* add */)
	}

	return selector, nil
}

// healthCheck detects a handler for its health.
// If detection times out, return false.
func healthCheck(handler fileop.DFSFileHandler) handlerStatus {
	running := make(chan bool)
	ticker := time.NewTicker(time.Duration(*healthCheckTimeout) * time.Second)
	defer func() {
		ticker.Stop()
		close(running)
	}()

	go func() {
		running <- handler.IsHealthy()
	}()

	select {
	case result := <-running:
		return NewHandlerStatus(result)
	case <-ticker.C:
		log.Printf("check handler %v expired", handler.Name())
		return statusFailure
	}
}

// copyFile copies a file from src to dst.
func copyFile(dst, src fileop.DFSFileHandler, info *FileRecoveryInfo) error {
	rf, err := src.Open(info.Fid, info.Domain)
	if err != nil {
		return err
	}
	defer rf.Close()

	wf, err := dst.Create(rf.GetFileInfo())
	if err != nil {
		return err
	}
	defer wf.Close()

	size, err := io.Copy(wf, rf)
	if err != nil {
		return err
	}

	if size != rf.GetFileInfo().Size {
		return fmt.Errorf("Copy file %s, size error: expected %d, actual %d",
			info.String(), rf.GetFileInfo().Size, size)
	}

	return nil
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
