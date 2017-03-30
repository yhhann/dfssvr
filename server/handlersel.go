package server

import (
	"flag"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/notice"
)

var (
	HealthCheckInterval = flag.Int("health-check-interval", 30, "health check interval in seconds.")
	HealthCheckTimeout  = flag.Int("health-check-timeout", 5, "health check timeout in seconds.")
	HealthCheckManually = flag.Bool("health-check-manually", true, "true for checking health manually.")

	recoveryBufferSize = flag.Int("recovery-buffer-size", 100000, "size of channel for each DFSFileHandler.")
	recoveryInterval   = flag.Int("recovery-interval", 60, "interval in seconds for recovery event inspection.")
	recoveryBatchSize  = flag.Int("recovery-batch-size", 100000, "batch size for recovery event inspection.")

	segmentDeletion = flag.Bool("segment-deletion", false, "true for remove segment.")
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

	backStoreShard *metadata.Shard
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
			glog.Warningf("Failed to close the old handler: %v", sh.handler.Name())
		}

		sh.Shutdown()
		hs.delShardHandler(handlerName)
	}

	glog.Infof("Succeeded to delete handler, shard: %s", handlerName)
}

func inferShardType(shard *metadata.Shard) {
	if shard.ShdType != metadata.RegularServer {
		return
	}

	if len(shard.VolHost) != 0 && len(shard.VolName) != 0 { // GlusterFS
		shard.ShdType = metadata.Glustergo
	} else { // GridFS
		shard.ShdType = metadata.Gridgo
	}
}

func (hs *HandlerSelector) addHandler(shard *metadata.Shard) {
	var handler fileop.DFSFileHandler
	var err error

	inferShardType(shard)

	switch shard.ShdType {
	case metadata.Glustra:
		handler, err = fileop.NewGlustraHandler(shard, filepath.Join(*logDir, shard.Name))
	case metadata.Glustergo:
		handler, err = fileop.NewGlusterHandler(shard, filepath.Join(*logDir, shard.Name))
	case metadata.Gridgo:
		handler, err = fileop.NewGridFsHandler(shard)
	case metadata.DegradeServer:
		handler, err = fileop.NewGridFsHandler(shard)
	case metadata.BackstoreServer:
		//skip
	default:
		err = fmt.Errorf("invalid shard type")
	}
	if err != nil {
		glog.Warningf("Failed to create handler, shard: %v, error: %v", shard, err)
		return
	}

	if shard.ShdType == metadata.DegradeServer {
		if hs.degradeShardHandler != nil {
			glog.Warningf("Failed to create degrade server, since we already have one, shard: %v", shard)
			return
		}

		dh := fileop.NewDegradeHandler(handler, hs.dfsServer.reOp)
		hs.degradeShardHandler = NewShardHandler(dh, statusOk, hs)
		glog.Infof("Succeeded to create degrade handler, shard: %s", shard.Name)
		return
	}

	if hs.backStoreShard != nil {
		handler = fileop.NewBackStoreHandler(handler, hs.backStoreShard)
		glog.Infof("Succeeded to attach handler %s with back store %s", handler.Name(), hs.backStoreShard.Name)
	}

	if sh, ok := hs.getShardHandler(handler.Name()); ok {
		if err := sh.Shutdown(); err != nil {
			glog.Warningf("Failed to shutdown old handler: %v", sh.handler.Name())
		}
		glog.Infof("Succeded to shutdown old handler, shard: %s", shard.Name)
	}

	sh := NewShardHandler(handler, statusOk, hs)

	hs.addRecovery(handler.Name(), sh.recoveryChan)

	glog.Infof("Succeeded to create handler, shard: %s", shard.Name)
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
		return nil, fmt.Errorf("handler is nil")
	}

	if status, ok := hs.getHandlerStatus(*handler); ok && status == statusOk {
		return handler, nil
	}

	dh := hs.degradeShardHandler.handler
	if hs.degradeShardHandler.status == statusOk {
		glog.Errorf("!!! server %s degrade to %s", (*handler).Name(), dh.Name())
		return &dh, nil
	}

	return nil, fmt.Errorf("'%s' and '%s' not reachable", (*handler).Name(), dh.Name())
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
		glog.Warningf("Ignored error: %v", err)
	}

	return n, m, nil
}

func (hs *HandlerSelector) updateHandlerStatus(h fileop.DFSFileHandler, status handlerStatus) {
	hs.handlerLock.Lock()
	defer hs.handlerLock.Unlock()

	hs.shardHandlers[h.Name()].updateStatus(status)
}

func (hs *HandlerSelector) getHandlerStatus(h fileop.DFSFileHandler) (handlerStatus, bool) {
	if *HealthCheckManually {
		// TODO(hanyh): update status from db.
		return statusOk, true
	}

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
		glog.Infof("Succeeded to start routine for checking shard servers.")

		for {
			select {
			case v := <-data:
				serverName := string(v)
				shard, err := s.mOp.LookupShardByName(serverName)
				if err != nil {
					if err == mgo.ErrNotFound {
						shard := &metadata.Shard{
							Name: serverName,
						}
						hs.updateHandler(shard, 2 /* delete handler */)
						break
					}
					glog.Warningf("Failed to lookup shard %s, error: %v", serverName, err)
					break
				}

				hs.updateHandler(shard, 1 /* add handler */)
			case err := <-errs:
				glog.Warningf("Failed to process shard notice, error: %v", err)
			}
		}
	}()

	go func() {
		data, errs := s.notice.CheckDataChange(notice.ShardChunkPath)
		glog.Infof("Succeeded to start routine for checking segment.")

		for {
			select {
			case v := <-data:
				segmentName := string(v)
				domain, err := strconv.Atoi(segmentName)
				if err != nil {
					glog.Warningf("Failed to convert segment number %s, error %v", segmentName, err)
					break
				}

				if domain == -1 {
					hs.backfillSegment()
					break
				}

				seg, err := s.mOp.LookupSegmentByDomain(int64(domain))
				if err != nil {
					glog.Warningf("Failed to lookup segment %d, error: %v", domain, err)
					break
				}

				// Update segment in selector.
				hs.updateSegment(seg)
			case err := <-errs:
				glog.Warningf("Failed to process segment notice, error: %v", err)
			}
		}
	}()
}

// startRecoveryRoutine starts a recovery routine for every handler.
func (hs *HandlerSelector) startRevoveryDispatchRoutine() {
	go func() {
		ticker := time.NewTicker(time.Duration(*recoveryInterval) * time.Second)
		defer ticker.Stop()
		glog.Infof("Succeeded to start routine for recovery dispatch.")

		for {
			select {
			case <-ticker.C:
				if err := hs.dispatchRecoveryEvent(*recoveryBatchSize, int64(*recoveryInterval)); err != nil {
					glog.Warningf("Recovery dispatch error, %v", err)
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
			glog.Warningf("Failed to get file handler for %d", e.Domain)
			continue
		}

		rec, ok := hs.getRecovery((*h).Name())
		if !ok {
			glog.Warningf("Failed to dispatch recovery event %s", e.String())
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

// updateSegments updates a segment.
func (hs *HandlerSelector) updateSegment(segment *metadata.Segment) {
	hs.segmentLock.Lock()
	defer hs.segmentLock.Unlock()

	hs.segments = updateSegment(hs.segments, segment)
}

func (hs *HandlerSelector) backfillSegment() {
	hs.segmentLock.Lock()
	defer hs.segmentLock.Unlock()

	hs.segments = hs.dfsServer.mOp.FindAllSegmentsOrderByDomain()
	glog.Infof("Succeeded to backfill segment %d.", len(hs.segments))
}

// FindPerfectSegment finds a perfect segment for domain.
// Segments must be in ascending order.
func (hs *HandlerSelector) FindPerfectSegment(domain int64) *metadata.Segment {
	hs.segmentLock.RLock()
	defer hs.segmentLock.RUnlock()

	_, result := findPerfectSegment(hs.segments, domain)
	return result
}

func NewHandlerSelector(dfsServer *DFSServer) (*HandlerSelector, error) {
	hs := new(HandlerSelector)
	hs.dfsServer = dfsServer

	hs.recoveries = make(map[string]chan *FileRecoveryInfo)
	hs.shardHandlers = make(map[string]*ShardHandler)

	// Fill segment data.
	hs.backfillSegment()
	for _, seg := range hs.segments {
		glog.Infof("Segment: [Domain:%d, ns:%s, ms:%s]",
			seg.Domain, seg.NormalServer, seg.MigrateServer)
	}

	// Initialize storage servers
	shards := hs.dfsServer.mOp.FindAllShards()

	for _, shard := range shards {
		if shard.ShdType == metadata.BackstoreServer {
			hs.backStoreShard = shard
			break
		}
	}

	for _, shard := range shards {
		if shard.ShdType == metadata.BackstoreServer {
			continue
		}
		hs.addHandler(shard)
	}

	return hs, nil
}

func updateSegment(segments []*metadata.Segment, segment *metadata.Segment) []*metadata.Segment {
	pos, result := findPerfectSegment(segments, segment.Domain)

	if result.Domain != segment.Domain { // Not found
		// Insert
		pos++
		rear := append([]*metadata.Segment{}, segments[pos:]...)
		segments = append(segments[:pos], segment)
		segments = append(segments, rear...)

		glog.Infof("Succeeded to insert segment [d:%d,n:%s,m:%s] at pos %d.", segment.Domain, segment.NormalServer, segment.MigrateServer, pos)
		return segments
	}

	// Found. Equal, remove it according to flag.
	if *segmentDeletion &&
		result.NormalServer == segment.NormalServer &&
		result.MigrateServer == segment.MigrateServer {
		segs := append(segments[:pos], segments[pos+1:]...)
		segments = segs
		glog.Infof("Succeeded to remove segment [d:%d,n:%s,m:%s] at pos %d.", segment.Domain, segment.NormalServer, segment.MigrateServer, pos)
		return segments
	}

	// Not equal, update it.
	segments[pos] = segment
	glog.Infof("Succeeded to update segment [d:%d,n:%s,m:%s] at pos %d.", segment.Domain, segment.NormalServer, segment.MigrateServer, pos)
	return segments
}

func findPerfectSegment(segments []*metadata.Segment, domain int64) (int, *metadata.Segment) {
	var pos int
	var result *metadata.Segment

	for i, seg := range segments {
		if domain > seg.Domain {
			pos, result = i, seg
			continue
		} else if domain == seg.Domain {
			pos, result = i, seg
			break
		} else {
			break
		}
	}

	return pos, result
}

// healthCheck detects a handler for its health.
// If detection times out, return false.
func healthCheck(handler fileop.DFSFileHandler) handlerStatus {
	running := make(chan bool, 1)
	ticker := time.NewTicker(time.Duration(*HealthCheckTimeout) * time.Second)
	defer func() {
		ticker.Stop()
	}()

	go func() {
		running <- handler.IsHealthy()
		close(running)
	}()

	select {
	case result := <-running:
		status := NewHandlerStatus(result)
		if status != statusOk {
			glog.Warningf("check handler %v %s", handler.Name(), status.String())
		}
		instrument.HealthCheckStatus <- &instrument.Measurements{
			Name:  handler.Name(),
			Biz:   status.String(),
			Value: 1.0,
		}
		return status
	case <-ticker.C:
		glog.Warningf("check handler %v expired", handler.Name())
		instrument.HealthCheckStatus <- &instrument.Measurements{
			Name:  handler.Name(),
			Biz:   "expired",
			Value: 1.0,
		}
		return statusFailure
	}
}
