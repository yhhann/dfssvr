package discovery

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"jingoal/dfs/notice"
)

const (
	// dfsPath is the path for dfs server to register.
	dfsPath = "/shard/dfs/dfs_"
)

// Register defines an action of underlying service register.
type Register interface {
	// Register registers the DfsServer
	Register(*DfsServer) error

	// GetDfsServerMap returns the map of DfsServer,
	// which will be updated in realtime.
	GetDfsServerMap() map[string]*DfsServer
}

// ZKDfsServerRegister implements the Register interface
type ZKDfsServerRegister struct {
	zkAddrs    []string
	zkTimeout  time.Duration
	serverMap  map[string]*DfsServer
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
	_, data, errors := r.notice.Register(dfsPath, serverData, true /*startCheckRoutine*/)

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
}

func NewZKDfsServerRegister(addrs string, timeout time.Duration) *ZKDfsServerRegister {
	r := new(ZKDfsServerRegister)
	r.zkAddrs = strings.Split(addrs, ",")
	r.zkTimeout = timeout
	r.notice = notice.NewDfsZK(r.zkAddrs, r.zkTimeout)
	r.serverMap = make(map[string]*DfsServer)

	return r
}
