package metadata

import (
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	EventCommand  EventType = iota
	CommandDelete           // 1
	SucCreate               // 2
	FailCreate              // 3
	SucDelete               // 4
	FailDelete              // 5
	SucRead                 // 6
	FailRead                // 7
	SucDupl                 // 8
	FailDupl                // 9
	SucMd5                  // 10
	FailMd5                 // 11
)

const (
	EVENT_COL = "event" // event collection name
)

type EventType uint

func (t EventType) String() string {
	switch t {
	default:
		return "None"
	case EventCommand:
		return "Command"
	case CommandDelete:
		return "CommandDelete"
	case SucCreate:
		return "SucCreate"
	case FailCreate:
		return "FailCreate"
	case SucDelete:
		return "SucDelete"
	case FailDelete:
		return "FailDelete"
	case SucRead:
		return "SucRead"
	case FailRead:
		return "FailRead"
	case SucDupl:
		return "SucDupl"
	case FailDupl:
		return "FailDupl"
	case SucMd5:
		return "SucMd5"
	case FailMd5:
		return "FailMd5"
	}
}

// Event represents an event, such as a successful reading or an other error.
type Event struct {
	Id          bson.ObjectId `bson:"_id"`                 // id
	Type        string        `bson:"eventType,omitempty"` // event type, for compatible with 1.0
	Timestamp   int64         `bson:"timeStamp"`           // timestamp
	EventId     string        `bson:"eventId,omitempty"`   // eventId, for compatible with 1.0
	ThreadId    string        `bson:"threadId,omitempty"`  // threadId, for compatible with 1.0
	Description string        `bson:"description"`         // description
	Domain      int64         `bson:"domain"`              // domain
	EType       EventType     `bson:"eType"`               // event type (dfs 2.0)
	Fid         string        `bson:"fid"`                 // fid
	Elapse      int64         `bson:"elapse,omitempty"`    // elapse in nanosecond
	// TODO(hanyh): Add a field to record node which generats this event.
}

type EventOp struct {
	uri    string
	dbName string

	session *mgo.Session
}

func (op *EventOp) execute(target func(session *mgo.Session) error) error {
	return ExecuteWithClone(op.session, target)
}

func (op *EventOp) Close() {
	op.session.Close()
}

// SaveEvent saves an event into database.
// If id of the saved object is nil, it will be set to a new ObjectId.
func (op *EventOp) SaveEvent(e *Event) error {
	if string(e.Id) == "" {
		e.Id = bson.NewObjectId()
	}
	if !e.Id.Valid() {
		return ObjectIdInvalidError
	}
	return op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(EVENT_COL).Insert(*e)
	})
}

// RemoveEvent removes an event by its id.
func (op *EventOp) RemoveEvent(id bson.ObjectId) error {
	return op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(EVENT_COL).RemoveId(id)
	})
}

// LookupEventById finds an event by its id.
func (op *EventOp) LookupEventById(id bson.ObjectId) (*Event, error) {
	e := new(Event)
	if err := op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(EVENT_COL).FindId(id).One(e)
	}); err != nil {
		return nil, err
	}

	return e, nil
}

// GetEvents gets an event iterator.
func (op *EventOp) GetEvents(eventType string, threadId string, start int64, end int64) *mgo.Iter {
	q := bson.M{"timeStamp": bson.M{"$gte": start, "$lte": end}}
	if strings.TrimSpace(eventType) != "" {
		q["eventType"] = eventType
	}
	if strings.TrimSpace(threadId) != "" {
		q["threadId"] = threadId
	}

	var iter *mgo.Iter
	op.execute(func(session *mgo.Session) error {
		iter = session.DB(op.dbName).C(EVENT_COL).Find(q).Iter()
		return nil
	})

	return iter
}

// NewEvnetOp creates a EventOp object with given mongodb uri
// and database name.
func NewEventOp(dbName string, uri string) (*EventOp, error) {
	session, err := CopySession(uri)
	if err != nil {
		return nil, err
	}

	return &EventOp{
		uri:     uri,
		dbName:  dbName,
		session: session,
	}, nil
}
