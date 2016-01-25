package server

import (
	"log"
	"testing"

	"jingoal/dfs/metadata"
)

func createSelector() (*HandlerSelector, error) {
	segments := []*metadata.Segment{
		&metadata.Segment{
			Domain:        1,
			NormalServer:  "shard-1",
			MigrateServer: "shard-2",
		},
		&metadata.Segment{
			Domain:       10,
			NormalServer: "shard-2",
		},
	}

	shards := []*metadata.Shard{
		&metadata.Shard{
			Name: "shard-1",
		},
		&metadata.Shard{
			Name: "shard-2",
		},
		&metadata.Shard{
			Name:    "degrade-svr",
			ShdType: 1,
		},
	}

	return NewHandlerSelector(segments, shards)
}

func TestNormal(t *testing.T) {
	sel, err := createSelector()
	if err != nil {
		t.Fatalf("create selector error %v", err)
	}

	wh, err := sel.getDFSFileHandlerForWrite(1)
	if err != nil {
		t.Fatalf("get write handler error %v", err)
	}
	if (*wh).Name() != "shard-2" {
		t.Fatalf("w:%+v", (*wh).Name())
	}

	n, m, err := sel.getDFSFileHandlerForRead(1)
	if err != nil {
		t.Fatalf("get read handler error %v", err)
	}
	if (*n).Name() != "shard-1" && (*m).Name() != "shard-1" {
		t.Fatalf("n:%+v, m:%+v", (*n).Name(), (*m).Name())
	}

	wh, err = sel.getDFSFileHandlerForWrite(11)
	if err != nil {
		t.Fatalf("get write handler error %v", err)
	}
	if (*wh).Name() != "shard-2" {
		t.Fatalf("w:%+v", (*wh).Name())
	}

	n, m, err = sel.getDFSFileHandlerForRead(11)
	if err != nil {
		t.Fatalf("get read handler error %v", err)
	}
	if (*n).Name() != "shard-2" {
		t.Fatalf("n:%+v", (*n).Name())
	}
	if m != nil {
		t.Fatalf("m:%+v", (*m).Name())
	}
}

func TestDegrade(t *testing.T) {
	sel, err := createSelector()
	if err != nil {
		t.Fatalf("create selector error %v", err)
	}
	sel.updateStatus("shard-2", statusFailure)

	wh, err := sel.getDFSFileHandlerForWrite(1)
	if err != nil {
		t.Fatalf("get write handler error %v", err)
	}
	if (*wh).Name() != "degrade-svr" {
		t.Fatalf("w:%+v", (*wh).Name())
	}

	n, m, err := sel.getDFSFileHandlerForRead(1)
	if err != nil {
		t.Fatalf("get read handler error %v", err)
	}
	if (*n).Name() != "shard-1" && (*m).Name() != "shard-1" {
		t.Fatalf("n:%+v, m:%+v", (*n).Name(), (*m).Name())
	}

	wh, err = sel.getDFSFileHandlerForWrite(11)
	if err != nil {
		t.Fatalf("get write handler error %v", err)
	}
	if (*wh).Name() != "degrade-svr" {
		t.Fatalf("w:%+v", (*wh).Name())
	}

	n, m, err = sel.getDFSFileHandlerForRead(11)
	if err != nil {
		t.Fatalf("get read handler error %v", err)
	}
	if (*n).Name() != "degrade-svr" {
		t.Fatalf("n:%+v", (*n).Name())
	}
	if m != nil {
		t.Fatalf("m:%+v", (*m).Name())
	}
}

func degrade(domain int64, t *testing.T, sel *HandlerSelector) {
	log.Printf("===domain: %d===\n", domain)
	wh, err := sel.getDFSFileHandlerForWrite(domain)
	if err != nil {
		t.Fatalf("get write handler error %v", err)
	}
	log.Printf("write:\t%+v", (*wh).Name())

	n, m, err := sel.getDFSFileHandlerForRead(domain)
	if err != nil {
		t.Fatalf("get read handler error %v", err)
	}
	log.Printf("read, n:\t%+v", (*n).Name())
	if m != nil {
		log.Printf("read, m:\t%+v", (*m).Name())
	} else {
		log.Printf("read, m:\tnil")
	}
}

func TestDynamic(t *testing.T) { // For manual check.
	sel, err := createSelector()
	if err != nil {
		t.Fatalf("create selector error %v", err)
	}

	sel.updateStatus("shard-2", statusFailure)
	degrade(1, t, sel)
	degrade(11, t, sel)

	sel.updateStatus("shard-2", statusOk)
	degrade(1, t, sel)
	degrade(11, t, sel)

	sel.updateStatus("shard-1", statusFailure)
	degrade(1, t, sel)
	degrade(11, t, sel)

	sel.updateStatus("shard-1", statusOk)
	degrade(1, t, sel)
	degrade(11, t, sel)
}
