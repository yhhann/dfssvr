package discovery

import (
	"encoding/json"
	"log"
	"path/filepath"
	"sync"

	"jingoal/dfs/notice"
	"jingoal/dfs/transfer"
)

const (
	// dfsPath is the path for dfs server to register.
	dfsPath = notice.ShardDfsPath + "/dfs_"
)

// Register defines an action of underlying service register.
type Register interface {
	// Register registers the DfsServer
	Register(*DfsServer) error

	// GetDfsServerMap returns the map of DfsServer,
	// which will be updated in realtime.
	GetDfsServerMap() map[string]*DfsServer

	// AddObserver adds an observer for DfsServer node changed.
	AddObserver(chan<- struct{}, string)

	// RemoveObserver removes an observer for DfsServer node changed.
	RemoveObserver(chan<- struct{})
}

// ZKDfsServerRegister implements the Register interface
type ZKDfsServerRegister struct {
	serverMap  map[string]*DfsServer
	observers  map[chan<- struct{}]string
	notice     notice.Notice
	rwmu       sync.RWMutex
	registered bool
}

// Register registers a DfsServer.
// If successed, other servers will be notified.
func (r *ZKDfsServerRegister) Register(s *DfsServer) error {
	if r.registered {
		log.Printf("server %v has registerd", s)
		return nil
	}

	serverData, err := json.Marshal(s)
	if err != nil {
		return err
	}

	// Register server
	nodeName, data, errors := r.notice.Register(dfsPath, serverData, true /*startCheckRoutine*/)
	transfer.NodeName = filepath.Base(nodeName)

	go func() {
		for {
			select {
			case changedServer := <-data: // Get changed server and update serverMap.
				server := new(DfsServer)
				if err := json.Unmarshal(changedServer, server); err != nil {
					log.Printf("json.Unmarshal error: %v", err)
					continue
				}
				r.putDfsServerToMap(server)
			case err := <-errors:
				log.Printf("notice routine error %v", err)
				if r.registered {
					r.registered = false
				}
				// TODO(hanyh): retry n times.
				return
			}
		}
	}()

	r.registered = true
	return nil
}

// GetDfsServerMap returns the map of DfsServer, which be update in realtime.
func (r *ZKDfsServerRegister) GetDfsServerMap() map[string]*DfsServer {
	r.rwmu.RLock()
	defer r.rwmu.RUnlock()

	return r.serverMap
}

func (r *ZKDfsServerRegister) putDfsServerToMap(server *DfsServer) {
	r.rwmu.Lock()
	defer r.rwmu.Unlock()

	r.serverMap[server.Id] = server

	// r.observers is a map which key hold channel.

	// When a client invokes method GetDfsServers, a new channel which
	// attached by the client will be added into r.observers, and when
	// server detects a client is offline, the channel that client
	// attached will be removed from r.observers.
	go func() {
		for ob := range r.observers {
			ob <- struct{}{}
		}
	}()
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
	r.serverMap = make(map[string]*DfsServer)
	r.observers = make(map[chan<- struct{}]string)

	return r
}
