package server

import (
	"fmt"

	"github.com/golang/glog"
	"golang.org/x/net/context"

	"jingoal.com/dfs/proto/transfer"
)

// Stat gets file info with given fid.
func (s *DFSServer) Stat(ctx context.Context, req *transfer.GetFileReq) (*transfer.PutFileRep, error) {
	serviceName := "Stat"
	peerAddr := getPeerAddressString(ctx)
	glog.Infof("%s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Id) == 0 || req.Domain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := bizFunc(s.statBiz).withDeadline(serviceName, ctx, req)
	if err != nil {
		return nil, err
	}

	result, ok := t.(*transfer.PutFileRep)
	if ok {
		return result, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) statBiz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	req, ok := r.(*transfer.GetFileReq)
	if !ok {
		return nil, AssertionError
	}

	_, file, err := s.openFileForRead(req.Id, req.Domain)
	if err != nil {
		return nil, err
	}
	file.Close()

	return &transfer.PutFileRep{
		File: file.GetFileInfo(),
	}, nil
}
