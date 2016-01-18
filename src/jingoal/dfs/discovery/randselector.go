package discovery

import (
	"errors"
	"math/rand"
	"sync"
)

var (
	ServerPoolEmptyErr = errors.New("server pool empty")
)

// RandSelector implements Selector, selects a DfsServer randomly.
// For client use, it will be implemented in java language.
type RandSelector struct {
	serverMap map[string]*DfsServer
	serverIds []string
	rwLock    sync.RWMutex
}

func (rs *RandSelector) GetPerfectServer() (*DfsServer, error) {
	rs.rwLock.RLock()
	defer rs.rwLock.RUnlock()

	if len(rs.serverIds) == 0 {
		return nil, ServerPoolEmptyErr
	}

	return rs.serverMap[rs.serverIds[rand.Intn(len(rs.serverIds))]], nil
}

func (rs *RandSelector) AddServer(s *DfsServer) {
	rs.rwLock.Lock()
	defer rs.rwLock.Unlock()

	if _, ok := rs.serverMap[s.Id]; !ok {
		rs.serverIds = append(rs.serverIds, s.Id)
	}
	rs.serverMap[s.Id] = s
}

func NewRandSelector() *RandSelector {
	rs := new(RandSelector)
	rs.serverMap = make(map[string]*DfsServer)
	rs.serverIds = make([]string, 0, 10)

	return rs
}
