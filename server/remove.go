package server

import (
	"fmt"
	"log"
	"time"

	"golang.org/x/net/context"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// Remove deletes a file.
func (s *DFSServer) RemoveFile(ctx context.Context, req *transfer.RemoveFileReq) (*transfer.RemoveFileRep, error) {
	serviceName := "RemoveFile"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("%s, client: %s, %v", serviceName, peerAddr, req)

	clientDesc := ""
	if req.GetDesc() != nil {
		clientDesc = req.GetDesc().Desc
	}

	if len(req.Id) == 0 || req.Domain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := bizFunc(s.removeBiz).withDeadline(serviceName, ctx, req, peerAddr, clientDesc)
	if err != nil {
		return nil, err
	}

	if result, ok := t.(*transfer.RemoveFileRep); ok {
		return result, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) removeBiz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("parameter number %d", len(args))
	}

	peerAddr, ok := args[0].(string)
	clientDesc, ok := args[1].(string)
	req, ok := r.(*transfer.RemoveFileReq)
	if !ok {
		return nil, AssertionError
	}

	startTime := time.Now()

	// log the remove command for audit.
	event := &metadata.Event{
		EType:     metadata.CommandDelete,
		Timestamp: util.GetTimeInMilliSecond(),
		Domain:    req.Domain,
		Fid:       req.Id,
		Elapse:    -1,
		Description: fmt.Sprintf("%s, client %s\n%s", metadata.CommandDelete.String(),
			peerAddr, clientDesc),
	}
	if er := s.eventOp.SaveEvent(event); er != nil {
		// log into file instead return.
		log.Printf("%s, error: %v", event.String(), er)
	}

	rep := &transfer.RemoveFileRep{}
	result := false

	var p fileop.DFSFileHandler
	var fm *fileop.FileMeta

	nh, mh, err := s.selector.getDFSFileHandlerForRead(req.Domain)
	if err != nil {
		log.Printf("RemoveFile, failed to get handler for read, error: %v", err)
		return rep, err
	}

	for _, h := range []*fileop.DFSFileHandler{nh, mh} {
		if h == nil {
			continue
		}

		p = *h
		result, fm, err = p.Remove(req.Id, req.Domain)
		if err != nil {
			log.Printf("RemoveFile, failed to remove file %s %d from %v, %v", req.Id, req.Domain, p.Name(), err)
		}
	}

	// space log.
	if result {
		fid, ok := fm.Id.(bson.ObjectId)
		if !ok {
			return nil, fmt.Errorf("Invalid id, %T, %v", fm.Id, fm.Id)
		}
		slog := &metadata.SpaceLog{
			Domain:    fm.Domain,
			Uid:       fm.UserId,
			Fid:       fid.Hex(),
			Biz:       fm.Biz,
			Size:      fm.Length,
			Timestamp: time.Now(),
			Type:      metadata.DeleteType.String(),
		}
		if er := s.spaceOp.SaveSpaceLog(slog); er != nil {
			log.Printf("%s, error: %v", slog.String(), er)
		}
	}

	// log the remove result for audit.
	resultEvent := &metadata.Event{
		EType:     metadata.SucDelete,
		Timestamp: util.GetTimeInMilliSecond(),
		Domain:    req.Domain,
		Fid:       req.Id,
		Elapse:    time.Since(startTime).Nanoseconds(),
		Description: fmt.Sprintf("%s, client %s, command %s, result %t, from %v",
			metadata.SucDelete.String(), peerAddr, event.Id.Hex(), result, p.Name()),
	}
	if er := s.eventOp.SaveEvent(resultEvent); er != nil {
		// log into file instead return.
		log.Printf("%s, error: %v", event.String(), er)
	}

	// TODO(hanyh): monitor remove.

	if result {
		log.Printf("RemoveFile, succeeded to remove entity %s from %v.", req.Id, p.Name())
	} else {
		log.Printf("RemoveFile, succeeded to remove reference %s from %v", req.Id, p.Name())
	}

	rep.Result = result
	return rep, nil
}
