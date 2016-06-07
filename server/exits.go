package server

import (
	"fmt"
	"log"

	"golang.org/x/net/context"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/proto/transfer"
)

// Exist checks existentiality of a file.
func (s *DFSServer) Exist(ctx context.Context, req *transfer.ExistReq) (*transfer.ExistRep, error) {
	serviceName := "Exist"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("service: %s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Id) == 0 || req.Domain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := withDeadline(serviceName, ctx, req, s.existBiz)

	if err != nil {
		return nil, err
	}

	if result, ok := t.(*transfer.ExistRep); ok {
		return result, err
	}

	return nil, AssertionError
}

func (s *DFSServer) exist(id string, domain int64) (result bool, err error) {
	defer func() {
		if err == fileop.FileNotFound {
			result, err = false, nil
		}
	}()

	_, file, err := s.searchFileForRead(id, domain)
	if err != nil {
		return
	}
	if file == nil {
		return
	}

	file.Close()
	result, err = true, nil
	return
}

func (s *DFSServer) existBiz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	req, ok := r.(*transfer.ExistReq)
	if !ok {
		return nil, AssertionError
	}

	result, err := s.exist(req.Id, req.Domain)
	if err != nil {
		log.Printf("Failed to exist %s, %d", req.Id, req.Domain)
	}

	return &transfer.ExistRep{
		Result: result,
	}, err
}
