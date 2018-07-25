package metadata

import (
	"testing"
	"time"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2/bson"
)

var (
	dbUri = "mongodb://192.168.64.176:27017?maxPoolSize=30"

	stableFid = "5b513e53bccf53000f11f81b"
)

func TestSaveOrUpdate(t *testing.T) {
	op, err := NewCacheLogOp("cachelog-test", dbUri)
	if err != nil {
		t.Errorf("NewCacheLogOp error %v", err)
	}

	cachelog := CacheLog{
		Timestamp: time.Now().Unix(),
		Domain:    2,
		Fid:       stableFid,
	}

	var newOplog *CacheLog
	var i int64
	for i = 0; i < 10; i++ {
		newOplog, err = op.SaveOrUpdate(&cachelog)
		if err != nil {
			t.Fatalf("SaveOrUpdate error %v", err)
		}
	}

	if err := op.RemoveAllCacheLog(newOplog); err != nil {
		t.Fatalf("RemoveAllCacheLog error %v", err)
	}

	if newOplog.RetryTimes != 10 {
		t.Fatalf("RetryTimes error %d", newOplog.RetryTimes)
	}
}

func TestGetCacheLogs(t *testing.T) {
	op, err := NewCacheLogOp("cachelog-test", dbUri)
	if err != nil {
		t.Errorf("NewCacheLogOp error %v", err)
	}

	cachelog := CacheLog{
		Timestamp: time.Now().Unix(),
		Domain:    2,
		Fid:       stableFid,
	}

	var iterCount int64 = 21
	newOplog := make([]*CacheLog, iterCount)
	var i int64
	for i = 0; i < iterCount; i++ {
		cachelog.Fid = bson.NewObjectId().Hex()
		newOplog[i], err = op.SaveOrUpdate(&cachelog)
		if err != nil {
			t.Fatalf("SaveOrUpdate error %v", err)
		}
	}

	iter, err := op.GetCacheLogs()
	if err != nil {
		t.Fatalf("GetCacheLogs error %v", err)
	}
	defer iter.Close()

	var count int64 = 0
	var ol CacheLog
	for iter.Next(&ol) {
		count++
	}

	iter.Close()

	glog.Infoln("count ", count)
	if count != iterCount {
		t.Fatal("GetCacheLogs count error ", count)
	}

	for _, cachelog := range newOplog {
		if err := op.RemoveAllCacheLog(cachelog); err != nil {
			t.Fatalf("RemoveAllCacheLog error %v", err)
		}
	}
}
