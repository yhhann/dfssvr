package server

import (
	"fmt"
	"io"
	"log"
	"time"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// GetFile gets a file from server.
func (s *DFSServer) GetFile(req *transfer.GetFileReq, stream transfer.FileTransfer_GetFileServer) (err error) {
	serviceName := "GetFile"
	peerAddr := getPeerAddressString(stream.Context())
	log.Printf("%s start, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Id) == 0 || req.Domain <= 0 {
		return fmt.Errorf("invalid request [%v]", req)
	}

	return withStreamDeadline(serviceName, req, stream, s.getFileStream, serviceName, peerAddr, s)
}

func (s *DFSServer) getFileStream(request interface{}, grpcStream interface{}, args []interface{}) error {
	startTime := time.Now()

	serviceName, peerAddr, err := extractStreamFuncParams(args)
	if err != nil {
		return err
	}

	req, stream, err := verifyFileStream(request, grpcStream)
	if err != nil {
		return err
	}

	_, file, err := s.searchFileForRead(req.Id, req.Domain)
	if err != nil {
		if err == fileop.FileNotFound {
			event := &metadata.Event{
				EType:       metadata.FailRead,
				Timestamp:   util.GetTimeInMilliSecond(),
				Domain:      req.Domain,
				Fid:         req.Id,
				Description: fmt.Sprintf("%s, client %s", metadata.FailRead.String(), peerAddr),
			}
			s.eventOp.SaveEvent(event)
		}
		return err
	}
	defer file.Close()

	// check timeout, for test.
	if dl, ok := getDeadline(stream); ok {
		given := dl.Sub(startTime)
		expected, err := checkTimeout(file.GetFileInfo().Size, rRate, given)
		if err != nil {
			log.Printf("%s, Timeout will happen, expected %v, given %v, return immediately", serviceName, expected, given)
			return err
		}
	}

	// First, we send file info.
	err = stream.Send(&transfer.GetFileRep{
		Result: &transfer.GetFileRep_Info{
			Info: file.GetFileInfo(),
		},
	})
	if err != nil {
		return err
	}

	// Second, we send file content in a loop.
	var off int64
	b := make([]byte, fileop.DefaultChunkSizeInBytes)
	for {
		length, err := file.Read(b)
		if err == io.EOF || (err == nil && length == 0) {
			nsecs := time.Since(startTime).Nanoseconds()
			rate := off * 8 * 1e6 / nsecs // in kbit/s

			instrumentGetFile(off, rate, serviceName)
			log.Printf("GetFile ok, %s, length %d, elapse %d, rate %d kbit/s", req, off, nsecs, rate)

			return nil
		}
		if err != nil {
			log.Printf("GetFile, read source error, %s, %v", req.Id, err)
			return err
		}
		err = stream.Send(&transfer.GetFileRep{
			Result: &transfer.GetFileRep_Chunk{
				Chunk: &transfer.Chunk{
					Pos:     off,
					Length:  int64(length),
					Payload: b[:length],
				},
			},
		})
		if err != nil {
			log.Printf("GetFile, send to client error, %s, %v", req.Id, err)
			return err
		}

		off += int64(length)
	}
}

func verifyFileStream(request interface{}, grpcStream interface{}) (req *transfer.GetFileReq, stream transfer.FileTransfer_GetFileServer, err error) {
	req, ok := request.(*transfer.GetFileReq)
	if !ok {
		return nil, nil, AssertionError
	}
	stream, ok = grpcStream.(transfer.FileTransfer_GetFileServer)
	if !ok {
		return nil, nil, AssertionError
	}

	return req, stream, nil
}