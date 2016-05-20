package discovery

import (
	"encoding/json"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"

	"jingoal.com/dfs/notice"
	dpb "jingoal.com/dfs/proto/discovery"
	"jingoal.com/dfs/proto/transfer"
)

const (
	// dfsPath is the path for dfs server to register.
	dfsPath = notice.ShardDfsPath + "/dfs_"
)

// Register defines an action of underlying service register.
type Register interface {
	// Register registers the DfsServer
	Register(*dpb.DfsServer) error

	// GetDfsServerMap returns the map of DfsServer,
	// which will be updated in realtime.
	GetDfsServerMap() map[string]*dpb.DfsServer

	// AddObserver adds an observer for DfsServer node changed.
	AddObserver(chan<- struct{}, string)

	// RemoveObserver removes an observer for DfsServer node changed.
	RemoveObserver(chan<- struct{})
}

// ZKDfsServerRegister implements the Register interface
type ZKDfsServerRegister struct {
	serverMap  map[string]*dpb.DfsServer
	observers  map[chan<- struct{}]string
	notice     notice.Notice
	rwmu       sync.RWMutex
	registered int32 // 1 for registered, 0 for not registered.
}

// Register registers a DfsServer.
// If successed, other servers will be notified.
func (r *ZKDfsServerRegister) Register(s *dpb.DfsServer) error {
	if atomic.LoadInt32(&r.registered) == 1 {
		log.Printf("Server %v has registerd already.", s)
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
				log.Printf("Succeeded to fire %d sendFlag.", len(r.observers))

			case changedServer := <-data: // Get changed server and update serverMap.
				server := new(dpb.DfsServer)
				if err := json.Unmarshal(changedServer, server); err != nil {
					log.Printf("Failed to unmarshal json, error: %v", err)
					continue
				}
				r.putDfsServerToMap(server)

			case err := <-errors:
				atomic.CompareAndSwapInt32(&r.registered, 1, 0)

				// Something must be done.
				log.Printf("Failed to do notice, routine exit. error: %v", err)
				return
			}
		}
	}()

	atomic.CompareAndSwapInt32(&r.registered, 0, 1)

	return nil
}

// GetDfsServerMap returns the map of DfsServer, which be update in realtime.
func (r *ZKDfsServerRegister) GetDfsServerMap() map[string]*dpb.DfsServer {
	r.rwmu.RLock()
	defer r.rwmu.RUnlock()

	return r.serverMap
}

func (r *ZKDfsServerRegister) putDfsServerToMap(server *dpb.DfsServer) {
	r.rwmu.Lock()
	defer r.rwmu.Unlock()

	r.serverMap[server.Id] = server
	log.Printf("Succeeded to add server %s into server map", server.String())
}

// CleanDfsServerMap cleans the map of DfsServer.
func (r *ZKDfsServerRegister) cleanDfsServerMap() {
	r.rwmu.Lock()
	defer r.rwmu.Unlock()

	initialSize := len(r.serverMap)
	r.serverMap = make(map[string]*dpb.DfsServer, initialSize)

	log.Printf("Succeeded to clean DfsServerMap, %d", initialSize)
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
	r.serverMap = make(map[string]*dpb.DfsServer)
	r.observers = make(map[chan<- struct{}]string)

	return r
}
