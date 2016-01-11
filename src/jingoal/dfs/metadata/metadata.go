// Package metadata processes metadata about shard, segment and event.
package metadata

import (
	"gopkg.in/mgo.v2/bson"
)

// Segment represents an interval of domain. Normally, files of these
// domains are located at NormalServer, when migrating files, the destination
// site is MigrateServer.
type Segment struct {
	Id            bson.ObjectId `bson:"_id"`                     // id
	Domain        int64         `bson:"domain"`                  // domain, cid
	NormalServer  string        `bson:"normalServer"`            // normal Site
	MigrateServer string        `bson:"migrateServer,omitempty"` // migrate Site
}

// Shard represents a storage shard server.
type Shard struct {
	Id          bson.ObjectId `bson:"_id"`                   // id
	Age         int64         `bson:"age"`                   // age
	Name        string        `bson:"name"`                  // server name
	Uri         string        `bson:"uri"`                   // uri
	MountPoint  string        `bson:"mountPoint,omitempty"`  // mount point
	PathVersion int           `bson:"pathVersion,omitempty"` // path version
	PathDigit   int           `bson:"pathDigit,omitempty"`   // path digit
	VolHost     string        `bson:"volHost,omitempty"`     // gfapi volume host
	VolName     string        `bson:"volName,omitempty"`     // gfapi volume name
	VolBase     string        `bson:"volBase,omitempty"`     // gfapi base dir
}

// Event represents an event, such as a successful reading or an other error.
type Event struct {
	Id          bson.ObjectId `bson:"_id"`         // id
	Type        string        `bson:"eventType"`   //event type
	Timestamp   int64         `bson:"timeStamp"`   //timestamp
	EventId     string        `bson:"eventId"`     // event id
	ThreadId    string        `bson:"threadId"`    // thread id
	Description string        `bson:"description"` // description
	Domain      int64         `bson:"domain"`      // domain
}

// FindPerfectSegment finds a perfect segment for domain.
// Segments must be in ascending order.
func FindPerfectSegment(segments []*Segment, domain int64) *Segment {
	var result *Segment
	for _, seg := range segments {
		if domain > seg.Domain {
			result = seg
			continue
		} else if domain == seg.Domain {
			result = seg
			break
		} else {
			break
		}
	}

	return result
}
