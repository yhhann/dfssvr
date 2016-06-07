package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// PutFile puts a file into server.
func (s *DFSServer) PutFile(stream transfer.FileTransfer_PutFileServer) error {
	serviceName := "PutFile"
	peerAddr := getPeerAddressString(stream.Context())

	return withStreamDeadline(serviceName, nil, stream, s.putFileStream, serviceName, peerAddr)
}

// putFileStream receives file content from client and saves to storage.
func (s *DFSServer) putFileStream(r interface{}, grpcStream interface{}, args []interface{}) error {
	var reqInfo *transfer.FileInfo
	var file fileop.DFSFile
	var length int
	var handler *fileop.DFSFileHandler

	stream, ok := grpcStream.(transfer.FileTransfer_PutFileServer)
	if !ok {
		return AssertionError
	}

	serviceName, peerAddr, err := extractStreamFuncParams(args)
	if err != nil {
		return err
	}

	startTime := time.Now()

	csize := 0
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if file == nil {
				log.Printf("PutFile error, no file info")
				return stream.SendAndClose(
					&transfer.PutFileRep{
						File: &transfer.FileInfo{
							Id: "no file info",
						},
					})
			}

			err := s.finishPutFile(file, handler, stream, startTime, serviceName, peerAddr)
			if err != nil {
				log.Printf("PutFile error, %v", err)
				return err
			}

			return nil
		}
		if err != nil {
			logInf := reqInfo
			if file != nil {
				logInf = file.GetFileInfo()
			}
			log.Printf("PutFile error, file %s, %v", logInf, err)
			return err
		}

		if file == nil {
			reqInfo = req.GetInfo()
			log.Printf("%s start, file info: %v, client: %s", serviceName, reqInfo, peerAddr)
			if reqInfo == nil {
				log.Printf("PutFile error, no file info")
				return errors.New("PutFile error: no file info")
			}

			file, handler, err = s.createFile(reqInfo, stream, startTime)
			if err != nil {
				log.Printf("PutFile error, create file %v, error %v", reqInfo, err)
				return err
			}
			defer file.Close()
		}

		csize, err = file.Write(req.GetChunk().Payload[:])
		if err != nil {
			return err
		}

		length += csize
		file.GetFileInfo().Size = int64(length)
	}
}

// finishPutFile sends receipt to client, saves event and space log.
func (s *DFSServer) finishPutFile(file fileop.DFSFile, handler *fileop.DFSFileHandler, stream transfer.FileTransfer_PutFileServer, startTime time.Time, serviceName string, peerAddr string) (err error) {
	inf := file.GetFileInfo()
	nsecs := time.Since(startTime).Nanoseconds()
	rate := inf.Size * 8 * 1e6 / nsecs // in kbit/s

	defer func() {
		if err != nil {
			(*handler).Remove(inf.Id, inf.Domain)
			err = fmt.Errorf("remove error: %v, client %s", err, peerAddr)
			return
		}
		log.Printf("PutFile, succeeded to finish file: %s, elapse %d, rate %d kbit/s\n", inf, nsecs, rate)
	}()

	// save a event for create file ok.
	event := &metadata.Event{
		EType:     metadata.SucCreate,
		Timestamp: util.GetTimeInMilliSecond(),
		Domain:    inf.Domain,
		Fid:       inf.Id,
		Elapse:    nsecs,
		Description: fmt.Sprintf("%s[PutFile], client: %s, dst: %s, size: %d",
			metadata.SucCreate.String(), peerAddr, (*handler).Name(), inf.Size),
	}
	err = s.eventOp.SaveEvent(event)
	if err != nil {
		err = fmt.Errorf("save event error: %v, client %s", err, peerAddr)
		return
	}

	err = stream.SendAndClose(
		&transfer.PutFileRep{
			File: inf,
		})
	if err != nil {
		err = fmt.Errorf("send receipt error: %v, client %s", err, peerAddr)
		return
	}

	log.Printf("PutFile, succeeded to send receipt %s to %s", inf.Id, peerAddr)

	slog := &metadata.SpaceLog{
		Domain:    inf.Domain,
		Uid:       fmt.Sprintf("%d", inf.User),
		Fid:       inf.Id,
		Biz:       inf.Biz,
		Size:      inf.Size,
		Timestamp: time.Now(),
		Type:      metadata.CreateType.String(),
	}
	err = s.spaceOp.SaveSpaceLog(slog)
	if err != nil {
		err = fmt.Errorf("save space log error: %v, client %s", err, peerAddr)
		return
	}

	instrumentPutFile(inf.Size, rate, serviceName)
	return
}

func (s *DFSServer) createFile(reqInfo *transfer.FileInfo, stream transfer.FileTransfer_PutFileServer, startTime time.Time) (fileop.DFSFile, *fileop.DFSFileHandler, error) {
	// check timeout, for test.
	if dl, ok := getDeadline(stream); ok {
		given := dl.Sub(startTime)
		_, err := checkTimeout(reqInfo.Size, wRate, given)
		if err != nil {
			return nil, nil, err
		}
	}

	handler, err := s.selector.getDFSFileHandlerForWrite(reqInfo.Domain)
	if err != nil {
		return nil, nil, err
	}

	file, err := (*handler).Create(reqInfo)
	if err != nil {
		return nil, nil, err
	}

	return file, handler, nil
}
func extractStreamFuncParams(args []interface{}) (sName string, pAddr string, err error) {
	if len(args) < 2 {
		err = fmt.Errorf("parameter number %d", len(args))
		return
	}

	ok := false
	sName, ok = args[0].(string)
	pAddr, ok = args[1].(string)
	if !ok {
		err = AssertionError
		return
	}

	return
}
