package metadata

import "testing"

const (
	dbUri  = "mongodb://192.168.55.193:27017?maxPoolSize=30"
	dbName = "shard"
)

func TestSegment(t *testing.T) {
	c := Segment{Domain: 200, NormalServer: "shard1", MigrateServer: "shard200"}

	sop, err := NewMongoMetaOp(dbName, dbUri)
	if err != nil {
		t.Errorf("init chunk col error: %v\n", err)
	}

	err = sop.SaveSegment(&c)
	if err != nil {
		t.Errorf("save chunk error: %v\n", err)
	}

	cf, err := sop.LookupSegmentByDomain(200)
	if err != nil {
		t.Errorf("find chunk error: %v\n", err)
	}
	if cf.Domain != c.Domain {
		t.Errorf("find chunk error: not expected.")
	}

	err = sop.RemoveSegment(200)
	if err != nil {
		t.Errorf("remove chunk error: %v\n", err)
	}
}

func TestFindAllSegmentsOrderByDomain(t *testing.T) {
	sop, err := NewMongoMetaOp(dbName, dbUri)
	if err != nil {
		t.Errorf("init chunk col error: %v\n", err)
	}

	chunks := sop.FindAllSegmentsOrderByDomain()
	if len(chunks) != 4 {
		t.Errorf("FindAllSegmentsOrderByDomain length error: %v\n", len(chunks))
	}
}

func TestShard(t *testing.T) {
	sop, err := NewMongoMetaOp(dbName, dbUri)
	if err != nil {
		t.Errorf("init server col error: %v\n", err)
	}

	s, err := sop.LookupShardByName("gluster1")
	if s == nil || err != nil {
		t.Errorf("find server shard1 error")
	}

	s, err = sop.LookupShardByName("shard-not-exist")
	if s != nil || err == nil {
		t.Errorf("find server not-exist error")
	}
}

func TestFindAllShards(t *testing.T) {
	sop, err := NewMongoMetaOp(dbName, dbUri)
	if err != nil {
		t.Errorf("init server col error: %v\n", err)
	}

	shards := sop.FindAllShards()
	if len(shards) != 5 {
		t.Errorf("find all servers error: %v\n", err)
	}

	found := false
	for _, shard := range shards {
		if shard.ShdType == DegradeServer {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("degrade server not found.")
	}
}
