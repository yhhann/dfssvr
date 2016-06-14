package server

import (
	"log"

	"golang.org/x/net/context"

	"jingoal.com/dfs/proto/transfer"
)

const (
	MaxChunkSize = 1048576 // Max chunk size in bytes.
	MinChunkSize = 1024    // Min chunk size in bytes.
)

// NegotiateChunkSize negotiates chunk size in bytes between client and server.
func (s *DFSServer) NegotiateChunkSize(ctx context.Context, req *transfer.NegotiateChunkSizeReq) (*transfer.NegotiateChunkSizeRep, error) {
	serviceName := "NegotiateChunkSize"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("%s, client: %s, %v", serviceName, peerAddr, req)

	t, err := withDeadline("NegotiateChunkSize", ctx, req, s.negotiateBiz)
	if err != nil {
		return nil, err
	}

	if rep, ok := t.(*transfer.NegotiateChunkSizeRep); ok {
		return rep, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) negotiateBiz(ctx interface{}, req interface{}, args []interface{}) (interface{}, error) {
	if r, ok := req.(*transfer.NegotiateChunkSizeReq); ok {
		rep := &transfer.NegotiateChunkSizeRep{
			Size: sanitizeChunkSize(r.Size),
		}
		return rep, nil
	}
	return nil, AssertionError
}

func sanitizeChunkSize(size int64) int64 {
	if size < MinChunkSize {
		return MinChunkSize
	}
	if size > MaxChunkSize {
		return MaxChunkSize
	}
	return size
}