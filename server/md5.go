package server

import (
	"fmt"
	"log"

	"golang.org/x/net/context"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// GetByMd5 gets a file by its md5.
func (s *DFSServer) GetByMd5(ctx context.Context, req *transfer.GetByMd5Req) (*transfer.GetByMd5Rep, error) {
	serviceName := "GetByMd5"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("%s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Md5) == 0 || req.Domain <= 0 || req.Size < 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := bizFunc(s.getByMd5Biz).withDeadline(serviceName, ctx, req, peerAddr)
	if err != nil {
		return nil, err
	}

	if result, ok := t.(*transfer.GetByMd5Rep); ok {
		return result, nil
	}

	return nil, AssertionError
}

// getByMd5Biz implements an instance of type bizFunc
// to process biz logic for getByMd5.
func (s *DFSServer) getByMd5Biz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("parameter number %d", len(args))
	}
	peerAddr, ok := args[0].(string)
	req, ok := r.(*transfer.GetByMd5Req)
	if !ok {
		return nil, AssertionError
	}

	p, oid, err := s.findByMd5(req.Md5, req.Domain, req.Size)
	if err != nil {
		log.Printf("Failed to find file by md5 [%s, %d, %d], error: %v", req.Md5, req.Domain, req.Size, err)
		return nil, err
	}

	did, err := p.Duplicate(oid)
	if err != nil {
		event := &metadata.Event{
			EType:       metadata.FailMd5,
			Timestamp:   util.GetTimeInMilliSecond(),
			Domain:      req.Domain,
			Fid:         oid,
			Description: fmt.Sprintf("%s, client %s", metadata.FailMd5.String(), peerAddr),
		}
		s.eventOp.SaveEvent(event)

		return nil, err
	}

	event := &metadata.Event{
		EType:       metadata.SucMd5,
		Timestamp:   util.GetTimeInMilliSecond(),
		Domain:      req.Domain,
		Fid:         oid,
		Description: fmt.Sprintf("%s, client %s, did %s", metadata.SucMd5.String(), peerAddr, did),
	}
	s.eventOp.SaveEvent(event)

	log.Printf("Succeeded to get file by md5, fid %v, md5 %v, domain %d, length %d",
		oid, req.Md5, req.Domain, req.Size)

	return &transfer.GetByMd5Rep{
		Fid: did,
	}, nil
}

// ExistByMd5 checks existentiality of a file.
func (s *DFSServer) ExistByMd5(ctx context.Context, req *transfer.GetByMd5Req) (*transfer.ExistRep, error) {
	serviceName := "ExistByMd5"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("%s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Md5) == 0 || req.Domain <= 0 || req.Size < 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := bizFunc(s.existByMd5Biz).withDeadline(serviceName, ctx, req)
	if err != nil {
		return nil, err
	}

	if result, ok := t.(*transfer.ExistRep); ok {
		return result, nil
	}

	return nil, AssertionError
}

// existByMd5Biz implements an instance of type bizFunc
// to process biz logic for existByMd5.
func (s *DFSServer) existByMd5Biz(c interface{}, r interface{}, args []interface{}) (interface{}, error) {
	req, ok := r.(*transfer.GetByMd5Req)
	if !ok {
		return nil, AssertionError
	}

	_, _, err := s.findByMd5(req.Md5, req.Domain, req.Size)
	if err != nil {
		log.Printf("Failed to find file by md5 [%s, %d, %d], error: %v", req.Md5, req.Domain, req.Size, err)
		return nil, err
	}

	return &transfer.ExistRep{
		Result: true,
	}, nil
}

func (s *DFSServer) findByMd5(md5 string, domain int64, size int64) (fileop.DFSFileHandler, string, error) {
	var err error
	nh, mh, err := s.selector.getDFSFileHandlerForRead(domain)
	if err != nil {
		log.Printf("Failed to get handler for read, error: %v", err)
		return nil, "", err
	}

	var p fileop.DFSFileHandler
	var oid string

	if mh != nil {
		p = *mh
		oid, err = p.FindByMd5(md5, domain, size)
		if err != nil {
			if nh != nil {
				p = *nh
				oid, err = p.FindByMd5(md5, domain, size)
				if err != nil {
					return nil, "", err // Not found in m and n.
				}
			} else {
				return nil, "", fileop.FileNotFound // Never reachs this line.
			}
		}
	} else if nh != nil {
		p = *nh
		oid, err = p.FindByMd5(md5, domain, size)
		if err != nil {
			return nil, "", err
		}
	}

	return p, oid, nil
}
