package metadata

import (
	"sync"
	"time"

	"gopkg.in/mgo.v2"

	"jingoal.com/dfs/instrument"
)

type sessionInstrument func() string

type Session struct {
	session *mgo.Session
	uri     string
}

var (
	mongoSessionManager *sessionManager
	sessionMap          map[*mgo.Session]sessionInstrument
	sessionLock         sync.Mutex
)

func init() {
	mongoSessionManager = newSessionManager()
	sessionMap = make(map[*mgo.Session]sessionInstrument)
}

// CopySession returns a session copied from the original session.
func CopySession(uri string) (*mgo.Session, error) {
	original, err := mongoSessionManager.getOrCreate(uri)
	if err != nil {
		return nil, err
	}

	session := original.Copy()

	me := instrument.Measurements{
		Name:  uri,
		Value: 0.0,
	}
	instrument.IncCopied <- &me
	sessionLock.Lock()
	sessionMap[session] = func() string {
		instrument.DecCopied <- &me
		return uri
	}
	sessionLock.Unlock()

	return session, nil
}

func ReleaseSession(session *mgo.Session) {
	sessionLock.Lock()
	if f, ok := sessionMap[session]; ok {
		delete(sessionMap, session)
		f()
	}
	sessionLock.Unlock()

	session.Close()
}

// CloneSession returns a session cloned from the original session.
func CloneSession(uri string) (*mgo.Session, error) {
	original, err := mongoSessionManager.getOrCreate(uri)
	if err != nil {
		return nil, err
	}

	session := original.Clone()

	me := instrument.Measurements{
		Name:  uri,
		Value: 0.0,
	}
	instrument.IncCloned <- &me
	sessionLock.Lock()
	sessionMap[session] = func() string {
		instrument.DecCloned <- &me
		return uri
	}
	sessionLock.Unlock()

	return session, nil

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

	me := instrument.Measurements{
		Name:  uri,
		Value: 0.0,
	}
	instrument.IncCreated <- &me
	sessionLock.Lock()
	sessionMap[session] = func() string {
		instrument.DecCreated <- &me
		return uri
	}
	sessionLock.Unlock()

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
