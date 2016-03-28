package server

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"gopkg.in/mgo.v2/bson"

	"jingoal/dfs/fileop"
	"jingoal/dfs/metadata"
	"jingoal/dfs/notice"
)

var (
	healthCheckInterval = flag.Int("health-check-interval", 30, "health check interval in seconds")
	healthCheckTimeout  = flag.Int("health-check-timeout", 5, "health check timeout in seconds")

	recoveryBufferSize = flag.Int("recovery-buffer-size", 100000, "size of channel for each DFSFileHandler")
	recoveryInterval   = flag.Int("recovery-interval", 60, "interval in seconds for recovery event inspection")
	recoveryBatchSize  = flag.Int("recovery-batch-size", 100000, "batch size for recovery event inspection")
)

// FileRecoveryInfo represents the information for file recovery.
type FileRecoveryInfo struct {
	Id     bson.ObjectId
	Fid    string
	Domain int64
}

func (rInfo *FileRecoveryInfo) String() string {
	return fmt.Sprintf("fid: %s, domain: %d, id: %s",
		rInfo.Fid, rInfo.Domain, rInfo.Id.Hex())
}

// HandlerSelector selects a perfect file handler for dfs server.
type HandlerSelector struct {
	segments    []*metadata.Segment
	segmentLock sync.RWMutex

	recoveries   map[string]chan *FileRecoveryInfo
	recoveryLock sync.RWMutex

	shardHandlers map[string]*ShardHandler
	handlerLock   sync.RWMutex

	degradeShardHandler *ShardHandler

	dfsServer *DFSServer
}

func (hs *HandlerSelector) addRecovery(name string, rInfo chan *FileRecoveryInfo) {
	hs.recoveryLock.Lock()
	defer hs.recoveryLock.Unlock()

	hs.recoveries[name] = rInfo
}

func (hs *HandlerSelector) delRecovery(name string) {
	hs.recoveryLock.Lock()
	defer hs.recoveryLock.Unlock()

	delete(hs.recoveries, name)
}

func (hs *HandlerSelector) getRecovery(name string) (chan *FileRecoveryInfo, bool) {
	hs.recoveryLock.RLock()
	defer hs.recoveryLock.RUnlock()

	rInfo, ok := hs.recoveries[name]

	return rInfo, ok
}

func (hs *HandlerSelector) setShardHandler(handlerName string, handler *ShardHandler) {
	hs.handlerLock.Lock()
	defer hs.handlerLock.Unlock()

	hs.shardHandlers[handlerName] = handler
}

func (hs *HandlerSelector) delShardHandler(handlerName string) {
	hs.handlerLock.Lock()
	defer hs.handlerLock.Unlock()

	delete(hs.shardHandlers, handlerName)
}

func (hs *HandlerSelector) getShardHandler(handlerName string) (*ShardHandler, bool) {
	hs.handlerLock.RLock()
	defer hs.handlerLock.RUnlock()

	h, ok := hs.shardHandlers[handlerName]
	return h, ok
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
	if sh, ok := hs.getShardHandler(handlerName); ok {
		if err := sh.handler.Close(); err != nil {
			log.Printf("Failed to close the old handler: %v", sh.handler.Name())
		}

		sh.Shutdown()
		hs.delShardHandler(handlerName)
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
		if hs.degradeShardHandler != nil {
			log.Printf("Failed to create degrade server, since we already have one, shard: %v", shard)
			return
		}

		dh := fileop.NewDegradeHandler(handler, hs.dfsServer.reOp)
		hs.degradeShardHandler = NewShardHandler(dh, statusOk, hs)
		log.Printf("Succeeded to create degrade handler, shard: %v", shard)
		return
	}

	if sh, ok := hs.getShardHandler(handler.Name()); ok {
		if err := sh.Shutdown(); err != nil {
			log.Printf("Failed to shutdown old handler: %v", sh.handler.Name())
		}
	}

	sh := NewShardHandler(handler, statusOk, hs)

	hs.setShardHandler(handler.Name(), sh)
	hs.addRecovery(handler.Name(), sh.recoveryChan)

	log.Printf("Succeeded to create handler, shard: %v", shard)
}

// getDfsFileHandler returns perfect file handlers to process file.
// The first returned handler is for normal handler,
// and the second one is for file migrating.
func (hs *HandlerSelector) getDFSFileHandler(domain int64) (*fileop.DFSFileHandler, *fileop.DFSFileHandler, error) {
	if len(hs.shardHandlers) == 0 {
		return nil, nil, fmt.Errorf("no handler")
	}

	seg := hs.FindPerfectSegment(domain)
	if seg == nil {
		return nil, nil, fmt.Errorf("can not find perfect server, domain %d", domain)
	}

	n, ok := hs.getShardHandler(seg.NormalServer)
	if !ok {
		return nil, nil, fmt.Errorf("no normal site, seg: %v", seg)
	}

	m, ok := hs.getShardHandler(seg.MigrateServer)
	if ok {
		return &(n.handler), &(m.handler), nil
	}

	return &(n.handler), nil, nil
}

// checkOrDegrade checks status of given handler,
// if status is offline, degrade.
func (hs *HandlerSelector) checkOrDegrade(handler *fileop.DFSFileHandler) (*fileop.DFSFileHandler, error) {
	if handler == nil { // Check for nil.
		return nil, fmt.Errorf("failed to degrade: handler is nil")
	}

	if status, ok := hs.getHandlerStatus(*handler); ok && status == statusOk {
		return handler, nil
	}

	dh := hs.degradeShardHandler.handler
	if hs.degradeShardHandler.status == statusOk {
		log.Printf("DEGRADE!!! %v ==> %v", (*handler).Name(), dh.Name())
		return &dh, nil
	}

	return nil, fmt.Errorf("failed to degrade: %v", (*handler).Name())
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
	// Need not return err, since we will verify the pair of n and m
	// outside this function.
	if err != nil {
		log.Printf("%v", err)
	}

	return n, m, nil
}

func (hs *HandlerSelector) updateHandlerStatus(h fileop.DFSFileHandler, status handlerStatus) {
	hs.handlerLock.Lock()
	defer hs.handlerLock.Unlock()

	hs.shardHandlers[h.Name()].updateStatus(status)
}

func (hs *HandlerSelector) getHandlerStatus(h fileop.DFSFileHandler) (handlerStatus, bool) {
	hs.handlerLock.RLock()
	defer hs.handlerLock.RUnlock()

	sh, ok := hs.shardHandlers[h.Name()]
	return sh.status, ok
}

// startShardNoticeRoutine starts a routine to receive and process notice
// from shard server and segment change.
func (hs *HandlerSelector) startShardNoticeRoutine() {
	s := hs.dfsServer
	go func() {
		data, errs := s.notice.CheckDataChange(notice.ShardServerPath)
		log.Printf("Succeeded to start routine for checking shard servers.")

		for {
			select {
			case v := <-data:
				serverName := string(v)
				shard, err := s.mOp.LookupShardByName(serverName)
				if err != nil {
					log.Printf("Failed to lookup shard by name %s", serverName)
					break
				}

				// TODO(hanyh): refine it.
				hs.updateHandler(shard, 1)
			case err := <-errs:
				log.Printf("Failed to process shard notice, error: %v", err)
			}
		}
	}()

	go func() {
		data, errs := s.notice.CheckDataChange(notice.ShardChunkPath)
		log.Printf("Succeeded to start routine for checking segment.")

		for {
			select {
			case v := <-data:
				segmentName := string(v)
				domain, err := strconv.Atoi(segmentName)
				if err != nil {
					log.Printf("Failed to get changed segment number, error %v", err)
					break
				}

				seg, err := s.mOp.LookupSegmentByDomain(int64(domain))
				if err != nil {
					log.Printf("Failed to get changed segment, error %v", err)
					break
				}

				// Update segment in selector.
				hs.updateSegment(seg)
			case err := <-errs:
				log.Printf("Failed to process segment notice, error: %v", err)
			}
		}
	}()
}

// startRecoveryRoutine starts a recovery routine for every handler.
func (hs *HandlerSelector) startRevoveryDispatchRoutine() {
	go func() {
		ticker := time.NewTicker(time.Duration(*recoveryInterval) * time.Second)
		defer ticker.Stop()
		log.Printf("Succeeded to start routine for recovery dispatch.")

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
	events, err := hs.dfsServer.reOp.GetEventsInBatch(batchSize, timeout)
	if err != nil {
		return err
	}

	for _, e := range events {
		h, err := hs.getDFSFileHandlerForWrite(e.Domain)
		if err != nil {
			log.Printf("Failed to get file handler for %d", e.Domain)
			continue
		}

		rec, ok := hs.getRecovery((*h).Name())
		if !ok {
			continue
		}

		rec <- &FileRecoveryInfo{
			Id:     e.Id,
			Fid:    e.Fid,
			Domain: e.Domain,
		}
	}

	return nil
}

func (hs *HandlerSelector) searchSegment(segment *metadata.Segment) int {
	for i, seg := range hs.segments {
		if seg.Domain == segment.Domain {
			return i
		}
	}

	return -1
}

// removeSegment removes a segment.
func (hs *HandlerSelector) removeSegment(segment *metadata.Segment) {
	pos := hs.searchSegment(segment)

	if pos < 0 { // Not found
		return
	}

	hs.segmentLock.Lock()
	defer hs.segmentLock.Unlock()

	segs := append(hs.segments[:pos], hs.segments[pos+1:]...)
	hs.segments = segs
}

// updateSegments updates a segment.
func (hs *HandlerSelector) updateSegment(segment *metadata.Segment) {
	pos := hs.searchSegment(segment)

	hs.segmentLock.Lock()
	defer hs.segmentLock.Unlock()

	if pos >= 0 { // Found
		foundSeg := hs.segments[pos]
		if foundSeg.NormalServer == segment.NormalServer &&
			foundSeg.MigrateServer == segment.MigrateServer {
			// Equal, remove it.
			segs := append(hs.segments[:pos], hs.segments[pos+1:]...)
			hs.segments = segs
			return
		}
		// Not equal, update it.
		hs.segments[pos] = segment
		return
	}

	// Not found, add it.
	hs.segments = append(hs.segments, segment)
}

// FindPerfectSegment finds a perfect segment for domain.
// Segments must be in ascending order.
func (hs *HandlerSelector) FindPerfectSegment(domain int64) *metadata.Segment {
	hs.segmentLock.RLock()
	defer hs.segmentLock.RUnlock()

	var result *metadata.Segment

	for _, seg := range hs.segments {
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

func NewHandlerSelector(dfsServer *DFSServer) (*HandlerSelector, error) {
	hs := new(HandlerSelector)
	hs.dfsServer = dfsServer

	hs.recoveries = make(map[string]chan *FileRecoveryInfo)
	hs.shardHandlers = make(map[string]*ShardHandler)

	// Fill segment data.
	hs.segments = hs.dfsServer.mOp.FindAllSegmentsOrderByDomain()
	for _, seg := range hs.segments {
		log.Printf("segment: [Domain:%d, ns:%s, ms:%s]",
			seg.Domain, seg.NormalServer, seg.MigrateServer)
	}

	// Initialize storage servers
	shards := hs.dfsServer.mOp.FindAllShards()

	for _, shard := range shards {
		hs.addHandler(shard)
	}

	return hs, nil
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
