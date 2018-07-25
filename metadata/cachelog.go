package metadata

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	CACHELOG_COL = "cachelog" // cachelog collection name

	CACHELOG_STATE_PENDING    = 0
	CACHELOG_STATE_PROCESSING = 1
	CACHELOG_STATE_FINISHED   = 2
)

// CacheLog represents an cache log.
type CacheLog struct {
	Id         bson.ObjectId `bson:"_id"`        // id, same as fid
	Timestamp  int64         `bson:"timestamp"`  // timestamp
	Domain     int64         `bson:"domain"`     // domain
	Fid        string        `bson:"fid"`        // fid
	RetryTimes int64         `bson:"retrytimes"` // retry times
	State      int64         `bson:"state"`      // state

	CacheId        string `bson:"cachefid"`    // cached fid
	CacheChunkSize int64  `bson:"cachecksize"` // cached chunk size
}

// String returns a string for CacheLog
func (g *CacheLog) String() string {
	return fmt.Sprintf("CacheLog[Fid %s, Domain %d, ReTry %d, State %d, %s]",
		g.Fid, g.Domain, g.RetryTimes, g.State, time.Unix(int64(g.Timestamp), 0).Format("2006-01-02 15:04:05"))
}

type CacheLogOp struct {
	uri    string
	dbName string
}

func (op *CacheLogOp) execute(target func(session *mgo.Session) error) error {
	s, err := CopySession(op.uri)
	if err != nil {
		return err
	}
	defer ReleaseSession(s)

	return target(s)
}

func (op *CacheLogOp) Close() {
}

// LookupCacheLogById finds an cache log by its id.
func (op *CacheLogOp) LookupCacheLogById(id bson.ObjectId) (*CacheLog, error) {
	log := &CacheLog{}
	if err := op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(CACHELOG_COL).FindId(id).One(log)
	}); err != nil {
		return nil, err
	}

	return log, nil
}

// GetCacheLogs gets an cache log iterator.
func (op *CacheLogOp) GetCacheLogs() (*mgo.Iter, error) {
	s, err := CopySession(op.uri)
	if err != nil {
		return nil, err
	}

	q := bson.M{"state": CACHELOG_STATE_PENDING}

	return s.DB(op.dbName).C(CACHELOG_COL).Find(q).Batch(20).Iter(), nil
}

// SaveOrUpdate saves cache log if not exists, or updates it if exists.
func (op *CacheLogOp) SaveOrUpdate(log *CacheLog) (*CacheLog, error) {
	change := mgo.Change{
		Update: bson.M{
			"$inc": bson.M{
				"retrytimes": 1,
			},
			"$set": bson.M{
				"fid":         log.Fid,
				"domain":      log.Domain,
				"timestamp":   log.Timestamp,
				"state":       log.State,
				"cachefid":    log.CacheId,
				"cachecksize": log.CacheChunkSize,
			},
		},
		Upsert:    true,
		ReturnNew: true,
	}

	result := &CacheLog{}
	if err := op.execute(func(session *mgo.Session) error {
		_, err := session.DB(op.dbName).C(CACHELOG_COL).Find(bson.M{"fid": log.Fid, "domain": log.Domain}).Apply(change, result)
		return err
	}); err != nil {
		return nil, err
	}

	return result, nil
}

// RemoveAllCacheLog removes cache logs by fid.
func (op *CacheLogOp) RemoveAllCacheLog(log *CacheLog) error {
	return op.execute(func(session *mgo.Session) error {
		_, err := session.DB(op.dbName).C(CACHELOG_COL).RemoveAll(bson.M{"fid": log.Fid, "domain": log.Domain})
		return err
	})
}

// RemoveCacheLogById removes an cache log by its id.
func (op *CacheLogOp) RemoveCacheLogById(id bson.ObjectId) error {
	return op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(CACHELOG_COL).RemoveId(id)
	})
}

// NewCacheLogOp creates an CacheLogOp object with given mongodb uri
// and database name.
func NewCacheLogOp(dbName string, uri string) (*CacheLogOp, error) {
	return &CacheLogOp{
		uri:    uri,
		dbName: dbName,
	}, nil
}
