package server

import (
	"fmt"
	"log"
	"time"

	"golang.org/x/net/context"

	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// Duplicate duplicates a file, returns a new fid.
func (s *DFSServer) Duplicate(ctx context.Context, req *transfer.DuplicateReq) (*transfer.DuplicateRep, error) {
	serviceName := "Duplicate"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("%s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Id) == 0 || req.Domain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := withDeadline(serviceName, ctx, req, s.duplicateBiz, peerAddr)
	if err != nil {
		return nil, err
	}

	result, ok := t.(*transfer.DuplicateRep)
	if ok {
		return result, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) duplicateBiz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("parameter number %d", len(args))
	}

	startTime := time.Now()

	peerAddr, ok := args[0].(string)
	req, ok := r.(*transfer.DuplicateReq)
	if !ok {
		return nil, AssertionError
	}

	did, err := s.duplicate(req.Id, req.Domain)
	if err != nil {
		event := &metadata.Event{
			EType:       metadata.FailDupl,
			Timestamp:   util.GetTimeInMilliSecond(),
			Domain:      req.Domain,
			Fid:         req.Id,
			Description: fmt.Sprintf("%s, client %s", metadata.FailDupl.String(), peerAddr),
			Elapse:      time.Since(startTime).Nanoseconds(),
		}
		s.eventOp.SaveEvent(event)

		log.Printf("Failed to duplicate %s[%d], error %v", req.Id, req.Domain, err)
		return nil, err
	}

	event := &metadata.Event{
		EType:       metadata.SucDupl,
		Timestamp:   util.GetTimeInMilliSecond(),
		Domain:      req.Domain,
		Fid:         req.Id,
		Description: fmt.Sprintf("%s, client %s, did %s", metadata.SucDupl.String(), peerAddr, did),
		Elapse:      time.Since(startTime).Nanoseconds(),
	}
	s.eventOp.SaveEvent(event)

	return &transfer.DuplicateRep{
		Id: did,
	}, nil
}

func (s *DFSServer) duplicate(oid string, domain int64) (string, error) {
	h, file, err := s.searchFileForRead(oid, domain)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// duplicate file from proper handler.
	did, err := h.Duplicate(oid)
	if err != nil {
		return "", err
	}

	return did, nil
}
