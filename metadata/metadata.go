// Package metadata processes metadata about shard, segment and event.
package metadata

import "gopkg.in/mgo.v2/bson"

const (
	RegularServer   ShardType = iota // Regular server.
	DegradeServer                    // Degrade server.
	BackstoreServer                  // Back store server.
)

// ShardType represents the type of a shard.
type ShardType uint

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
	ShdType     ShardType     `bson:"shdType,omitempty"`     // shard type
	MasterUri   string        `bson:"masterUri,omitempty"`   // master uri
	Replica     string        `bson:"replica,omitempty"`     // replica
}
