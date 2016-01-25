package metadata

import (
	"fmt"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
)

const (
	dbUri = "mongodb://192.168.55.193:27017?maxPoolSize=30"
)

func TestSegment(t *testing.T) {
	c := Segment{Domain: 200, NormalServer: "shard1", MigrateServer: "shard200"}

	sop, err := NewMongoMetaOp("shard", dbUri)
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
	sop, err := NewMongoMetaOp("shard", dbUri)
	if err != nil {
		t.Errorf("init chunk col error: %v\n", err)
	}

	chunks := sop.FindAllSegmentsOrderByDomain()
	if len(chunks) != 4 {
		t.Errorf("FindAllSegmentsOrderByDomain length error: %v\n", len(chunks))
	}
}

func TestShard(t *testing.T) {
	sop, err := NewMongoMetaOp("shard", dbUri)
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
	sop, err := NewMongoMetaOp("shard", dbUri)
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

func TestEvent(t *testing.T) {
	sop, err := NewMongoMetaOp("shard", dbUri)
	if err != nil {
		t.Errorf("init event col error %v\n", err)
	}

	id := bson.NewObjectId()
	e := Event{Id: id, Type: "testType", Timestamp: time.Now().Unix(),
		EventId: "event-test", ThreadId: "thread-test", Description: "delete", Domain: -1}

	if err := sop.SaveEvent(&e); err != nil {
		t.Errorf("save event error %v\n", err)
	}

	ef, _ := sop.LookupEventById(id)
	if ef.Id != e.Id {
		t.Errorf("find event error %v\n", err)
	}

	if err := sop.RemoveEvent(id); err != nil {
		t.Errorf("remove event error %v\n", err)
	}
}

func TestGetEvents(t *testing.T) {
	fid := fmt.Sprintf("%x", string(bson.NewObjectId()))
	sop, err := NewMongoMetaOp("shard", dbUri)
	if err != nil {
		t.Errorf("NewMongoMetaOp() error %v\n", err)
	}

	start := time.Now().Unix()
	time.Sleep(1 * time.Second)

	id1 := bson.NewObjectId()
	e1 := Event{Id: id1, Type: "testType", Timestamp: time.Now().Unix(),
		EventId: fid, ThreadId: "2000", Description: "create", Domain: -1}
	sop.SaveEvent(&e1)

	id2 := bson.NewObjectId()
	e2 := Event{Id: id2, Type: "testType", Timestamp: time.Now().Unix(),
		EventId: fid, ThreadId: "2000", Description: "read", Domain: -1}
	sop.SaveEvent(&e2)

	id3 := bson.NewObjectId()
	e3 := Event{Id: id3, Type: "testType1", Timestamp: time.Now().Unix(),
		EventId: fid, ThreadId: "2000", Description: "delete", Domain: -1}
	sop.SaveEvent(&e3)

	time.Sleep(1 * time.Second)
	end := time.Now().Unix()

	var e Event
	cnt := 0
	iter := sop.GetEvents("", "", start, end)
	for iter.Next(&e) {
		cnt++
	}
	iter.Close()
	if cnt != 0 {
		t.Errorf("GetEvent error, %d", cnt)
	}

	cnt = 0
	iter = sop.GetEvents("", "2000", start, end)
	for iter.Next(&e) {
		cnt++
	}
	if cnt != 0 {
		t.Errorf("GetEvent error, %d", cnt)
	}

	cnt = 0
	iter = sop.GetEvents("testType", "", start, end)
	for iter.Next(&e) {
		cnt++
	}
	if cnt != 0 {
		t.Errorf("GetEvent error, %d", cnt)
	}

	cnt = 0
	iter = sop.GetEvents("testType1", "2000", start, end)
	for iter.Next(&e) {
		cnt++
	}
	if cnt != 1 {
		t.Errorf("GetEvent error, %d", cnt)
	}

	cnt = 0
	iter = sop.GetEvents("testType", "2000", start, end)
	for iter.Next(&e) {
		cnt++
	}
	if cnt != 2 {
		t.Errorf("GetEvent error, %d", cnt)
	}

	sop.RemoveEvent(id1)
	sop.RemoveEvent(id2)
	sop.RemoveEvent(id3)
}

func toString(o interface{}) string {
	return fmt.Sprintf("%v", o)
}
