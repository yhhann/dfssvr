package metadata

import (
	"sync"
	"time"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2"
)

var (
	mongoSessionManager *sessionManager
)

func init() {
	mongoSessionManager = newSessionManager()
}

// CopySession returns a session copied from the original session.
func CopySession(uri string) (*mgo.Session, error) {
	original, err := mongoSessionManager.getOrCreate(uri)
	if err != nil {
		return nil, err
	}
	// TODO(hanyh): monitor
	return original.Copy(), nil
}

// CloneSession returns a session cloned from the original session.
func CloneSession(uri string) (*mgo.Session, error) {
	original, err := mongoSessionManager.getOrCreate(uri)
	if err != nil {
		return nil, err
	}
	// TODO(hanyh): monitor
	return original.Clone(), nil
}

// GetSession returns a singleton instance of session for every uri.
func GetSession(uri string) (*mgo.Session, error) {
	return mongoSessionManager.getOrCreate(uri)
}

// sessionManager generates a singlton instance for every uri.
type sessionManager struct {
	ss   map[string]*mgo.Session
	lock sync.RWMutex
}

func (sm *sessionManager) get(uri string) *mgo.Session {
	sm.lock.RLock()
	defer sm.lock.RUnlock()

	s, ok := sm.ss[uri]
	if ok {
		return s
	}

	return nil
}

// getOrCreate returns a singlton instance of session for every uri.
func (sm *sessionManager) getOrCreate(uri string) (*mgo.Session, error) {
	s := sm.get(uri)
	if s != nil {
		return s, nil
	}

	sm.lock.Lock()
	defer sm.lock.Unlock()

	s, ok := sm.ss[uri]
	if ok {
		return s, nil
	}

	session, err := openMongoSession(uri)
	if err != nil {
		return nil, err
	}

	// TODO(hanyh): monitor the session.
	glog.Infof("Succeeded to create session to %s", uri)
	sm.ss[uri] = session

	return session, nil
}

func newSessionManager() *sessionManager {
	return &sessionManager{
		ss: make(map[string]*mgo.Session),
	}
}

// openMongoSession returns a session by given mongodb uri.
func openMongoSession(uri string) (*mgo.Session, error) {
	info, err := mgo.ParseURL(uri)
	if err != nil {
		return nil, err
	}

	info.Timeout = time.Duration(*MongoTimeout) * time.Second
	info.FailFast = true
	session, err := mgo.DialWithInfo(info)
	if err != nil {
		return nil, err
	}

	if len(info.Addrs) > 1 {
		session.SetSafe(&mgo.Safe{WMode: "majority"})
	}

	return session, nil
}
