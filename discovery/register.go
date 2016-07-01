package discovery

import (
	"encoding/json"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/golang/glog"

	"jingoal.com/dfs/notice"
	"jingoal.com/dfs/proto/discovery"
	"jingoal.com/dfs/proto/transfer"
)

const (
	// dfsPath is the path for dfs server to register.
	dfsPath = notice.ShardDfsPath + "/dfs_"
)

// Register defines an action of underlying service register.
type Register interface {
	// Register registers the DfsServer
	Register(*discovery.DfsServer) error

	// GetDfsServerMap returns the map of DfsServer,
	// which will be updated in realtime.
	GetDfsServerMap() map[string]*discovery.DfsServer

	// AddObserver adds an observer for DfsServer node changed.
	AddObserver(chan<- struct{}, string)

	// RemoveObserver removes an observer for DfsServer node changed.
	RemoveObserver(chan<- struct{})
}

// ZKDfsServerRegister implements the Register interface
type ZKDfsServerRegister struct {
	serverMap  map[string]*discovery.DfsServer
	observers  map[chan<- struct{}]string
	notice     notice.Notice
	rwmu       sync.RWMutex
	registered int32 // 1 for registered, 0 for not registered.
}

// Register registers a DfsServer.
// If successed, other servers will be notified.
func (r *ZKDfsServerRegister) Register(s *discovery.DfsServer) error {
	if atomic.LoadInt32(&r.registered) == 1 {
		glog.Warningf("Server %v has registerd already.", s)
		return nil
	}

	serverData, err := json.Marshal(s)
	if err != nil {
		return err
	}

	// Register server
	nodeName, data, errors, clearFlag, sendFlag := r.notice.Register(dfsPath, serverData, true /*startCheckRoutine*/)
	transfer.NodeName = filepath.Base(nodeName)

	go func() {
		for {
			select {
			case <-clearFlag:
				r.cleanDfsServerMap()

			case <-sendFlag:
				// r.observers is a map from key to channel.

				// When a client invokes method GetDfsServers, a new channel which
				// attached by the client will be added into r.observers, and when
				// server detects a client is offline, the channel that client
				// attached will be removed from r.observers.
				go func() {
					for ob := range r.observers {
						ob <- struct{}{}
					}
				}()
				glog.Infof("Succeeded to fire online notice %d.", len(r.observers))

			case changedServer := <-data: // Get changed server and update serverMap.
				server := new(discovery.DfsServer)
				if err := json.Unmarshal(changedServer, server); err != nil {
					glog.Warningf("Failed to unmarshal json, error: %v", err)
					continue
				}
				r.putDfsServerToMap(server)

			case err := <-errors:
				atomic.CompareAndSwapInt32(&r.registered, 1, 0)

				// Something must be done.
				glog.Warningf("Watcher routine exit. error: %v", err)
				return
			}
		}
	}()

	atomic.CompareAndSwapInt32(&r.registered, 0, 1)

	return nil
}

// GetDfsServerMap returns the map of DfsServer, which be update in realtime.
func (r *ZKDfsServerRegister) GetDfsServerMap() map[string]*discovery.DfsServer {
	r.rwmu.RLock()
	defer r.rwmu.RUnlock()

	return r.serverMap
}

func (r *ZKDfsServerRegister) putDfsServerToMap(server *discovery.DfsServer) {
	r.rwmu.Lock()
	defer r.rwmu.Unlock()

	r.serverMap[server.Id] = server
	glog.Infof("Succeeded to add server %s into server map", server.String())
}

// CleanDfsServerMap cleans the map of DfsServer.
func (r *ZKDfsServerRegister) cleanDfsServerMap() {
	r.rwmu.Lock()
	defer r.rwmu.Unlock()

	initialSize := len(r.serverMap)
	r.serverMap = make(map[string]*discovery.DfsServer, initialSize)

	glog.Infof("Succeeded to clean DfsServerMap, %d", initialSize)
}

// AddObserver adds an observer for DfsServer node changed.
func (r *ZKDfsServerRegister) AddObserver(observer chan<- struct{}, name string) {
	observer <- struct{}{}
	r.observers[observer] = name
}

// RemoveObserver removes an observer for DfsServer node changed.
func (r *ZKDfsServerRegister) RemoveObserver(observer chan<- struct{}) {
	delete(r.observers, observer)
	close(observer)
}

func NewZKDfsServerRegister(notice notice.Notice) *ZKDfsServerRegister {
	r := new(ZKDfsServerRegister)
	r.notice = notice
	r.serverMap = make(map[string]*discovery.DfsServer)
	r.observers = make(map[chan<- struct{}]string)

	return r
}
