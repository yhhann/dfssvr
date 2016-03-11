package server

import (
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"jingoal/dfs/fileop"
)

const (
	statusOk      handlerStatus = iota // handler for shard is ok.
	statusFailure                      // handler for shard is failure.
)

type handlerStatus uint

func (hs handlerStatus) String() string {
	if hs == statusOk {
		return "ok"
	}

	return "failure"
}

func NewHandlerStatus(v bool) handlerStatus {
	if v {
		return statusOk
	}

	return statusFailure
}

// ShardHandler is a handler for shard, which
// hold all data and operator for a shard.
type ShardHandler struct {
	status  handlerStatus
	handler fileop.DFSFileHandler

	recoveryChan    chan *FileRecoveryInfo
	recoveryRunning int32 // 1 for running, 0 for not.

	healthyCheckRoutineRunning chan struct{} // For stopping healty check routine.

	hs *HandlerSelector
}

func (sh *ShardHandler) updateStatus(newStatus handlerStatus) {
	if sh.status == statusOk && newStatus != statusOk {
		sh.status = newStatus
		sh.stopRecoveryRoutine()
	}

	if sh.status != statusOk && newStatus == statusOk {
		sh.status = newStatus
		sh.startRecoveryRoutine()
	}
}

func (sh *ShardHandler) startRecoveryRoutine() {
	if atomic.CompareAndSwapInt32(&sh.recoveryRunning, 0 /*old value*/, 1 /*new value*/) {
		go sh.Recovery()
		log.Printf("Succeeded to start recovery routine for %s", sh.handler.Name())
	}
}

func (sh *ShardHandler) stopRecoveryRoutine() {
	if atomic.CompareAndSwapInt32(&sh.recoveryRunning, 1 /* old value*/, 0 /* new value*/) {
		close(sh.recoveryChan)
		log.Printf("Succeeded to stop recovery routine for %s", sh.handler.Name())
	}
}

func (sh *ShardHandler) Shutdown() error {
	sh.hs.delRecovery(sh.handler.Name())
	sh.stopRecoveryRoutine()

	// Stop healthy check routine.
	sh.healthyCheckRoutineRunning <- struct{}{}

	return sh.handler.Close()
}

// startHealthyCheckRoutine starts a routine for health check.
func (sh *ShardHandler) startHealthyCheckRoutine() {
	// Starts a routine for health check every healthCheckInterval seconds.
	go func() {
		ticker := time.NewTicker(time.Duration(*healthCheckInterval) * time.Second)
		defer ticker.Stop()

		fh := sh.handler

		for {
			select {
			case <-ticker.C:
				// Starts a routine to check server.
				if fh != nil {
					go func() {
						status := healthCheck(fh)
						sh.hs.updateHandlerStatus(fh, status)
						log.Printf("Status of handler %v is %s", fh.Name(), status.String())
					}()
				}
			case <-sh.healthyCheckRoutineRunning: // stop signal
				log.Printf("Succeeded to stop healty check routine for %v", sh.handler.Name())
				return
			}
		}
	}()

	log.Printf("Succeeded to start healthy check routine for %v, time interval %d seconds.",
		sh.handler.Name(), *healthCheckInterval)
}

// Recovery recoveries files from degradeHandler to a perfect normale handler.
func (sh *ShardHandler) Recovery() {
Stop:
	for {
		select {
		case recoveryInfo, ok := <-sh.recoveryChan:
			if !ok { // Routine will stop once channel close.
				break Stop
			}

			if err := copyFile(sh.handler, sh.hs.degradeShardHandler.handler, recoveryInfo); err != nil {
				log.Printf("Failed to recovery file %s, error: %v", recoveryInfo.Fid, err)
				break
			}

			if err := sh.hs.dfsServer.reOp.RemoveEvent(recoveryInfo.Id); err != nil {
				log.Printf("Failed to remove recovery event %s", recoveryInfo.String())
			}

			log.Printf("Succeeded to recovery file %s", recoveryInfo.Fid)
		}
	}

	log.Printf("Succeeded to stop recovery routine for %vis ", sh.handler.Name())
}

func NewShardHandler(handler fileop.DFSFileHandler, status handlerStatus, selector *HandlerSelector) *ShardHandler {
	sh := &ShardHandler{
		hs:                         selector,
		status:                     status,
		handler:                    handler,
		recoveryChan:               make(chan *FileRecoveryInfo, *recoveryBufferSize),
		healthyCheckRoutineRunning: make(chan struct{}),
	}

	sh.hs.addRecovery(sh.handler.Name(), sh.recoveryChan)
	sh.hs.setShardHandler(sh.handler.Name(), sh)
	sh.startHealthyCheckRoutine()

	return sh
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
		return fmt.Errorf("copy file %s, size error: expected %d, actual %d",
			info.String(), rf.GetFileInfo().Size, size)
	}

	return nil
}
