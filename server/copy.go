package server

import (
	"fmt"
	"io"
	"log"
	"time"

	"golang.org/x/net/context"

	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// Copy copies a file and returns its fid.
func (s *DFSServer) Copy(ctx context.Context, req *transfer.CopyReq) (*transfer.CopyRep, error) {
	serviceName := "Copy"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("%s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.SrcFid) == 0 || req.SrcDomain <= 0 || req.DstDomain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := withDeadline(serviceName, ctx, req, s.copyBiz, peerAddr)
	if err != nil {
		return nil, err
	}

	result, ok := t.(*transfer.CopyRep)
	if ok {
		return result, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) copyBiz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("parameter number %d", len(args))
	}
	peerAddr, ok := args[0].(string)
	req, ok := r.(*transfer.CopyReq)
	if !ok {
		return nil, AssertionError
	}

	if req.SrcDomain == req.DstDomain {
		did, err := s.duplicate(req.SrcFid, req.DstDomain)
		if err != nil {
			return nil, err
		}

		log.Printf("Copy is converted to duplicate, srcId: %s, srcDomain: %d, dstDomain: %d",
			req.SrcFid, req.SrcDomain, req.DstDomain)

		return &transfer.CopyRep{
			Fid: did,
		}, nil
	}

	startTime := time.Now()

	_, rf, err := s.openFileForRead(req.SrcFid, req.SrcDomain)
	if err != nil {
		return nil, err
	}
	defer rf.Close()

	// open destination file.
	handler, err := s.selector.getDFSFileHandlerForWrite(req.DstDomain)
	if err != nil {
		return nil, err
	}

	wf, err := (*handler).Create(&transfer.FileInfo{
		Domain: req.DstDomain,
		User:   req.DstUid,
		Biz:    req.DstBiz,
	})
	if err != nil {
		return nil, err
	}

	defer wf.Close()

	length, err := io.Copy(wf, rf)
	if err != nil {
		return nil, err
	}

	inf := wf.GetFileInfo()
	log.Printf("Succeeded to copy file %s to %s", req.SrcFid, inf.Id)

	// space log.
	slog := &metadata.SpaceLog{
		Domain:    inf.Domain,
		Uid:       fmt.Sprintf("%d", inf.User),
		Fid:       inf.Id,
		Biz:       inf.Biz,
		Size:      length,
		Timestamp: time.Now(),
		Type:      metadata.CreateType.String(),
	}
	s.spaceOp.SaveSpaceLog(slog)

	event := &metadata.Event{
		EType:     metadata.SucCreate,
		Timestamp: util.GetTimeInMilliSecond(),
		Domain:    inf.Domain,
		Fid:       inf.Id,
		Elapse:    time.Since(startTime).Nanoseconds(),
		Description: fmt.Sprintf("%s[Copy], client: %s, srcFid: %s, dst: %s", metadata.SucCreate.String(),
			peerAddr, req.SrcFid, (*handler).Name()),
	}
	s.eventOp.SaveEvent(event)

	return &transfer.CopyRep{
		Fid: inf.Id,
	}, nil
}
