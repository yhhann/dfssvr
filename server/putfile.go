package server

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/golang/glog"

	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/util"
)

// PutFile puts a file into server.
func (s *DFSServer) PutFile(stream transfer.FileTransfer_PutFileServer) error {
	serviceName := "PutFile"
	peerAddr := getPeerAddressString(stream.Context())

	return streamFunc(s.putFileStream).withStreamDeadline(serviceName, nil, stream, serviceName, peerAddr)
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
				glog.Warningf("PutFile error, no file info")
				return stream.SendAndClose(
					&transfer.PutFileRep{
						File: &transfer.FileInfo{
							Id: "no file info",
						},
					})
			}

			err := s.finishPutFile(file, handler, stream, startTime, serviceName, peerAddr)
			if err != nil {
				glog.Warningf("PutFile error, %v", err)
				return err
			}

			return nil
		}
		if err != nil {
			logInf := reqInfo
			if file != nil {
				logInf = file.GetFileInfo()
			}
			glog.Warningf("PutFile error, file %s, %v", logInf, err)
			return err
		}

		if file == nil {
			reqInfo = req.GetInfo()
			glog.Infof("%s start, file info: %v, client: %s", serviceName, reqInfo, peerAddr)
			if reqInfo == nil {
				glog.Warningf("PutFile error, no file info")
				return errors.New("PutFile error: no file info")
			}

			file, handler, err = s.createFile(reqInfo, stream, startTime)
			if err != nil {
				glog.Warningf("PutFile error, create file %v, error %v", reqInfo, err)
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
	nsecs := time.Since(startTime).Nanoseconds() + 1
	rate := inf.Size * 8 * 1e6 / nsecs // in kbit/s

	defer func() {
		if err != nil {
			if _, _, er := (*handler).Remove(inf.Id, inf.Domain); er != nil {
				glog.Warningf("remove error: %v, client %s", er, peerAddr)
			}
			return
		}
		glog.Infof("PutFile, succeeded to finish file: %s, elapse %d, rate %d kbit/s\n", inf, nsecs, rate)
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
	if er := s.eventOp.SaveEvent(event); er != nil {
		// log into file instead return.
		glog.Warningf("%s, error: %v", event.String(), er)
	}

	err = stream.SendAndClose(
		&transfer.PutFileRep{
			File: inf,
		})
	if err != nil {
		err = fmt.Errorf("send receipt error: %v, client %s", err, peerAddr)
		return
	}

	slog := &metadata.SpaceLog{
		Domain:    inf.Domain,
		Uid:       fmt.Sprintf("%d", inf.User),
		Fid:       inf.Id,
		Biz:       inf.Biz,
		Size:      inf.Size,
		Timestamp: time.Now(),
		Type:      metadata.CreateType.String(),
	}
	if er := s.spaceOp.SaveSpaceLog(slog); er != nil {
		glog.Warningf("%s, error: %v", slog.String(), er)
	}

	instrumentPutFile(inf.Size, rate, serviceName, inf.Biz)
	return
}

func (s *DFSServer) createFile(reqInfo *transfer.FileInfo, stream transfer.FileTransfer_PutFileServer, startTime time.Time) (fileop.DFSFile, *fileop.DFSFileHandler, error) {
	// check timeout, for test.
	if *enablePreJudge {
		if dl, ok := getDeadline(stream); ok {
			given := dl.Sub(startTime)
			expected, err := checkTimeout(reqInfo.Size, wRate, given)
			if err != nil {
				instrument.PrejudgeExceed <- &instrument.Measurements{
					Name:  "PutFile",
					Value: float64(expected.Nanoseconds()),
				}
				glog.Warningf("PutFile, timeout return early, expected %v, given %v", expected, given)
				return nil, nil, err
			}
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
