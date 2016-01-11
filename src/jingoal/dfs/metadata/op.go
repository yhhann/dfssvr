package metadata

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// MetaOp represents the operator of metadata.
type MetaOp interface {
	// Segment operators

	// SaveSegment saves a segment into database.
	SaveSegment(c *Segment) error

	// UpdateSegment updates segment.
	UpdateSegment(c *Segment) error

	// LookupSegmentByDomain finds a segment.
	LookupSegmentByDomain(domain int64) (*Segment, error)

	// FindNextDomainSegment finds the first segment
	// its domain greater than the given domain.
	FindNextDomainSegment(domain int64) (*Segment, error)

	// RemoveSegment removes a segment.
	RemoveSegment(domain int64) error

	// FindAllSegmentOrderByDomain finds all segment from database.
	FindAllSegmentsOrderByDomain() []*Segment

	// Shard operators

	// LookupShardByName finds a shard server by its name.
	LookupShardByName(name string) (*Shard, error)

	// FindAllShards finds all shard servers.
	FindAllShards() []*Shard

	// Event operators

	// SaveEvent saves an event.
	SaveEvent(e *Event) error

	// RemoveEvent removes an event by its id.
	RemoveEvent(id bson.ObjectId) error

	// LookupEventById finds an event by its id.
	LookupEventById(id bson.ObjectId) (*Event, error)

	// GetEvents gets an event iterator.
	GetEvents(eventType string, threadId string, start int64, end int64) *mgo.Iter
}
