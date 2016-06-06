package metadata

import (
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	CreateType SpaceLogType = iota
	DeleteType
)

// SpaceLogType represents the type of space log.
type SpaceLogType uint

func (t SpaceLogType) String() string {
	if t == CreateType {
		return "create"
	}

	return "delete"
}

func NewSpaceLogType(v string) SpaceLogType {
	if v == "create" {
		return CreateType
	}

	return DeleteType
}

// SpaceLog represents space log in db.
type SpaceLog struct {
	Id        bson.ObjectId `bson:"_id"`       // id
	Domain    int64         `bson:"domain"`    // domain
	Uid       string        `bson:"uid"`       // User id
	Fid       string        `bson:"fid"`       // File id
	Biz       string        `bson:"biz"`       // Biz
	Size      int64         `bson:"size"`      // length
	Timestamp time.Time     `bson:"timestamp"` // timestamp
	Type      string        `bson:"type"`      // type
}

type SpaceLogOp struct {
	uri    string
	dbName string

	session *mgo.Session
}

func (op *SpaceLogOp) execute(target func(*mgo.Session) error) error {
	return ExecuteWithClone(op.session, target)
}

func (op *SpaceLogOp) Close() {
	op.session.Close()
}

func (op *SpaceLogOp) SaveSpaceLog(log *SpaceLog) error {
	if string(log.Id) == "" {
		log.Id = bson.NewObjectId()
	}
	if !log.Id.Valid() {
		return ObjectIdInvalidError
	}
	if log.Timestamp.IsZero() {
		log.Timestamp = time.Now()
	}
	if strings.TrimSpace(log.Biz) == "" {
		log.Biz = "general"
	}

	return op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C("slog").Insert(*log)
	})
}

// NewSpaceLogOp creates a SpaceLogOp object with given mongodb uri
// and database name.
func NewSpaceLogOp(dbName string, uri string) (*SpaceLogOp, error) {
	session, err := CopySession(uri)
	if err != nil {
		return nil, err
	}

	return &SpaceLogOp{
		uri:     uri,
		dbName:  dbName,
		session: session,
	}, nil
}
