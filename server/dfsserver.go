// Package server implements DFSServer.
package server

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/transport"
	"gopkg.in/mgo.v2/bson"

	disc "jingoal.com/dfs/discovery"
	"jingoal.com/dfs/fileop"
	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/notice"
	"jingoal.com/dfs/proto/discovery"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/recovery"
	"jingoal.com/dfs/util"
)

var (
	logDir            = flag.String("gluster-log-dir", "/var/log/dfs", "gluster log file dir")
	heartbeatInterval = flag.Int("hb-interval", 5, "time interval in seconds of heart beat")

	RegisterAddr    = flag.String("register-addr", "", "register address")
	DefaultDuration = flag.Int("default-duration", 5, "default transfer duration in seconds.")
)

var (
	AssertionError = errors.New("assertion error")
)

var (
	rRate = 0.0 // kbit/s
	wRate = 0.0 // kbit/s
)

const (
	MaxChunkSize = 1048576 // Max chunk size in bytes.
	MinChunkSize = 1024    // Min chunk size in bytes.
)

// DFSServer implements DiscoveryServiceServer and FileTransferServer.
type DFSServer struct {
	mOp      metadata.MetaOp
	spaceOp  *metadata.SpaceLogOp
	eventOp  *metadata.EventOp
	reOp     *recovery.RecoveryEventOp
	register disc.Register
	notice   notice.Notice
	selector *HandlerSelector
}

// GetDfsServers gets a list of DfsServer from server.
func (s *DFSServer) GetDfsServers(req *discovery.GetDfsServersReq, stream discovery.DiscoveryService_GetDfsServersServer) error {
	clientId := strings.Join([]string{req.GetClient().Id, getPeerAddressString(stream.Context())}, "/")

	observer := make(chan struct{}, 100)
	s.register.AddObserver(observer, clientId)

	log.Printf("Client connected successfully, client: %s", clientId)

	ticker := time.NewTicker(time.Duration(*heartbeatInterval) * time.Second)
outLoop:
	for {
		select {
		case <-observer:
			if err := s.sendDfsServerMap(req, stream); err != nil {
				break outLoop
			}

		case <-ticker.C:
			if err := s.sendHeartbeat(req, stream); err != nil {
				break outLoop
			}
		}
	}

	ticker.Stop()
	s.register.RemoveObserver(observer)
	log.Printf("Client connection closed, client: %s", clientId)

	return nil
}

func (s *DFSServer) sendHeartbeat(req *discovery.GetDfsServersReq, stream discovery.DiscoveryService_GetDfsServersServer) error {
	rep := &discovery.GetDfsServersRep{
		GetDfsServerUnion: &discovery.GetDfsServersRep_Hb{
			Hb: &discovery.Heartbeat{
				Timestamp: time.Now().Unix(),
			},
		},
	}

	if err := stream.Send(rep); err != nil {
		return err
	}

	return nil
}

func (s *DFSServer) sendDfsServerMap(req *discovery.GetDfsServersReq, stream discovery.DiscoveryService_GetDfsServersServer) error {
	sm := s.register.GetDfsServerMap()
	ss := make([]*discovery.DfsServer, 0, len(sm))
	for _, pd := range sm {
		// If we detect a server offline, we set its value to nil,
		// so we must filter nil values out.
		if pd != nil {
			ss = append(ss, pd)
		}
	}

	rep := &discovery.GetDfsServersRep{
		GetDfsServerUnion: &discovery.GetDfsServersRep_Sl{
			Sl: &discovery.DfsServerList{
				Server: ss,
			},
		},
	}

	if err := stream.Send(rep); err != nil {
		return err
	}

	clientId := strings.Join([]string{req.GetClient().Id, getPeerAddressString(stream.Context())}, "/")
	log.Printf("Succeeded to send DfsServers to client: %s, Servers:", clientId)
	for i, s := range ss {
		log.Printf("%d. DfsServer: %s\n", i, strings.Join([]string{s.Id, s.Uri, s.Status.String()}, "/"))
	}

	return nil
}

// NegotiateChunkSize negotiates chunk size in bytes between client and server.
func (s *DFSServer) NegotiateChunkSize(ctx context.Context, req *transfer.NegotiateChunkSizeReq) (*transfer.NegotiateChunkSizeRep, error) {
	serviceName := "NegotiateChunkSize"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("service: %s, client: %s, %v", serviceName, peerAddr, req)

	t, err := withDeadline("NegotiateChunkSize", ctx, req, func(ctx interface{}, req interface{}) (interface{}, error) {
		if r, ok := req.(*transfer.NegotiateChunkSizeReq); ok {
			rep := &transfer.NegotiateChunkSizeRep{
				Size: sanitizeChunkSize(r.Size),
			}

			return rep, nil
		}

		return nil, AssertionError
	})

	if err != nil {
		return nil, err
	}

	if rep, ok := t.(*transfer.NegotiateChunkSizeRep); ok {
		return rep, nil
	}

	return nil, AssertionError
}

func finishRecv(info *transfer.FileInfo, stream transfer.FileTransfer_PutFileServer) error {
	return stream.SendAndClose(
		&transfer.PutFileRep{
			File: info,
		})
}

// PutFile puts a file into server.
func (s *DFSServer) PutFile(stream transfer.FileTransfer_PutFileServer) error {
	serviceName := "PutFile"
	peerAddr := getPeerAddressString(stream.Context())
	log.Printf("service: %s, client: %s", serviceName, peerAddr)

	return withStreamDeadline(serviceName, nil, stream, func(r interface{}, streem interface{}) error {
		var reqInfo *transfer.FileInfo
		var file fileop.DFSFile
		var length int
		var handler *fileop.DFSFileHandler

		stream, ok := streem.(transfer.FileTransfer_PutFileServer)
		if !ok {
			return AssertionError
		}

		startTime := time.Now()

		csize := 0
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				if file == nil {
					// TODO(hanyh): save an event for create file error.
					log.Println("Failed to save file: no file info")
					return finishRecv(
						&transfer.FileInfo{
							Id: "recv error: no file info",
						}, stream)
				}

				inf := file.GetFileInfo()
				err = finishRecv(file.GetFileInfo(), stream)
				if err == nil {
					slog := &metadata.SpaceLog{
						Domain:    inf.Domain,
						Uid:       fmt.Sprintf("%d", inf.User),
						Fid:       inf.Id,
						Biz:       inf.Biz,
						Size:      int64(length),
						Timestamp: time.Now(),
						Type:      metadata.CreateType.String(),
					}
					s.spaceOp.SaveSpaceLog(slog)

					nsecs := time.Since(startTime).Nanoseconds()
					// save a event for create file ok.
					event := &metadata.Event{
						EType:     metadata.SucCreate,
						Timestamp: util.GetTimeInMilliSecond(),
						Domain:    inf.Domain,
						Fid:       inf.Id,
						Elapse:    nsecs,
						Description: fmt.Sprintf("%s[PutFile], client: %s, dst: %s, size: %d",
							metadata.SucCreate.String(), peerAddr, (*handler).Name(), length),
					}
					s.eventOp.SaveEvent(event)

					rate := int64(length) * 8 * 1e6 / nsecs // in kbit/s
					instrument.FileSize <- &instrument.Measurements{
						Name:  serviceName,
						Value: float64(length),
					}
					instrument.TransferRate <- &instrument.Measurements{
						Name:  serviceName,
						Value: float64(rate),
					}

					log.Printf("Succeeded to save file: %s, elapse %d, rate %d kbit/s\n",
						inf, nsecs, rate)
					return nil
				}

				(*handler).Remove(inf.Id, inf.Domain)
			}
			if err != nil {
				logInf := reqInfo
				if file != nil {
					logInf = file.GetFileInfo()
				}
				log.Printf("Failed to save file: %s, %v\n", logInf, err)
				return err
			}

			if file == nil {
				reqInfo = req.GetInfo()
				log.Printf("%s, file info: %v, client: %s", serviceName, reqInfo, peerAddr)
				if reqInfo == nil {
					log.Printf("recv error: no file info")
					return errors.New("recv error: no file info")
				}

				// check timeout, for test.
				if dl, ok := getDeadline(stream); ok {
					given := dl.Sub(startTime)
					expected, err := checkTimeout(reqInfo.Size, wRate, given)
					if err != nil {
						log.Printf("%s, Timeout will happen, expected %v, given %v, return early", serviceName, expected, given)
						return err
					}
				}

				handler, err = s.selector.getDFSFileHandlerForWrite(reqInfo.Domain)
				if err != nil {
					log.Printf("get handler for write error: %v", err)
					return err
				}

				file, err = (*handler).Create(reqInfo)
				if err != nil {
					log.Printf("Create error, file: %s, error: %v\n", reqInfo, err)
					return err
				}

				defer file.Close()
			}

			csize, err = file.Write(req.GetChunk().Payload[:])
			if err != nil {
				return err
			}

			length += csize
		}
	})
}

func searchFile(id string, domain int64, nh fileop.DFSFileHandler, mh fileop.DFSFileHandler) (fileop.DFSFileHandler, fileop.DFSFile, error) {
	var h fileop.DFSFileHandler

	if mh != nil && nh != nil {
		h = mh
		file, err := mh.Open(id, domain)
		if err != nil { // Need not to check mgo.ErrNotFound
			h = nh
			file, err = nh.Open(id, domain)
		}
		return h, file, err
	} else if mh == nil && nh != nil {
		f, err := nh.Open(id, domain)
		return nh, f, err
	} else {
		return nil, nil, fmt.Errorf("get file error: normal site is nil")
	}
}

// GetFile gets a file from server.
func (s *DFSServer) GetFile(req *transfer.GetFileReq, stream transfer.FileTransfer_GetFileServer) (err error) {
	serviceName := "GetFile"
	peerAddr := getPeerAddressString(stream.Context())
	log.Printf("service: %s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Id) == 0 || req.Domain <= 0 {
		return fmt.Errorf("invalid request [%v]", req)
	}

	return withStreamDeadline(serviceName, req, stream, func(r interface{}, streem interface{}) error {
		startTime := time.Now()

		req, ok := r.(*transfer.GetFileReq)
		if !ok {
			return AssertionError
		}
		stream, ok := streem.(transfer.FileTransfer_GetFileServer)
		if !ok {
			return AssertionError
		}

		nh, mh, err := s.selector.getDFSFileHandlerForRead(req.Domain)
		if err != nil {
			log.Printf("Failed to get handler for read, error: %v", err)
			return err
		}

		var m fileop.DFSFileHandler
		if mh != nil {
			m = *mh
		}

		_, file, err := searchFile(req.Id, req.Domain, *nh, m)
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

				instrument.FileSize <- &instrument.Measurements{
					Name:  serviceName,
					Value: float64(off),
				}
				instrument.TransferRate <- &instrument.Measurements{
					Name:  serviceName,
					Value: float64(rate),
				}
				log.Printf("Succeeded to read file: %s, length %d, elapse %d, rate %d kbit/s",
					req, off, nsecs, rate)

				return nil
			}
			if err != nil {
				log.Printf("Failed to read file, %s, error: %v", req.Id, err)
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
				return err
			}

			off += int64(length)
		}
	})
}

// Remove deletes a file.
func (s *DFSServer) RemoveFile(ctx context.Context, req *transfer.RemoveFileReq) (*transfer.RemoveFileRep, error) {
	serviceName := "RemoveFile"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("service: %s, client: %s, %v", serviceName, peerAddr, req)

	clientDesc := ""
	if req.GetDesc() != nil {
		clientDesc = req.GetDesc().Desc
	}

	if len(req.Id) == 0 || req.Domain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := withDeadline(serviceName, ctx, req, func(c interface{}, r interface{}) (interface{}, error) {
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
		s.eventOp.SaveEvent(event)

		rep := &transfer.RemoveFileRep{}
		result := false

		var p fileop.DFSFileHandler
		var fm *fileop.FileMeta

		nh, mh, err := s.selector.getDFSFileHandlerForRead(req.Domain)
		if err != nil {
			log.Printf("Failed to get handler for read, error: %v", err)
			return rep, err
		}

		if nh != nil {
			p = *nh
			result, fm, err = p.Remove(req.Id, req.Domain)
			if err != nil {
				log.Printf("Failed to remove file %s from %v", req.Id, p.Name())
			}
		}

		if mh != nil {
			p = *mh
			result, fm, err = p.Remove(req.Id, req.Domain)
			if err != nil {
				log.Printf("Failed to remove file %s from %v", req.Id, p.Name())
			}
		}

		// space log.
		if result {
			fid, ok := fm.Id.(bson.ObjectId)
			if !ok {
				log.Printf("Failed to convert file to string, %T", fm.Id)
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
			s.spaceOp.SaveSpaceLog(slog)
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
		s.eventOp.SaveEvent(resultEvent)

		if result {
			log.Printf("Succeeded to remove entity %s from %v.", req.Id, p.Name())
		} else {
			log.Printf("Succeeded to remove reference %s from %v", req.Id, p.Name())
		}

		rep.Result = result
		return rep, nil
	})

	if err != nil {
		return nil, err
	}

	if result, ok := t.(*transfer.RemoveFileRep); ok {
		return result, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) duplicate(oid string, domain int64) (string, error) {
	nh, mh, err := s.selector.getDFSFileHandlerForRead(domain)
	if err != nil {
		return "", err
	}

	var m fileop.DFSFileHandler
	if mh != nil {
		m = *mh
	}

	h, file, err := searchFile(oid, domain, *nh, m)
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

// Duplicate duplicates a file, returns a new fid.
func (s *DFSServer) Duplicate(ctx context.Context, req *transfer.DuplicateReq) (*transfer.DuplicateRep, error) {
	serviceName := "Duplicate"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("service: %s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Id) == 0 || req.Domain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := withDeadline(serviceName, ctx, req, func(c interface{}, r interface{}) (interface{}, error) {
		startTime := time.Now()
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

	})

	if err != nil {
		return nil, err
	}

	result, ok := t.(*transfer.DuplicateRep)
	if ok {
		return result, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) exist(id string, domain int64) (result bool, err error) {
	defer func() {
		if err == fileop.FileNotFound {
			result, err = false, nil
		}
	}()

	nh, mh, err := s.selector.getDFSFileHandlerForRead(domain)
	if err != nil {
		return
	}

	var m fileop.DFSFileHandler
	if mh != nil {
		m = *mh
	}

	_, file, err := searchFile(id, domain, *nh, m)
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

// Exist checks existentiality of a file.
func (s *DFSServer) Exist(ctx context.Context, req *transfer.ExistReq) (*transfer.ExistRep, error) {
	serviceName := "Exist"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("service: %s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Id) == 0 || req.Domain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := withDeadline(serviceName, ctx, req, func(c interface{}, r interface{}) (interface{}, error) {
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
	})

	if err != nil {
		return nil, err
	}

	if result, ok := t.(*transfer.ExistRep); ok {
		return result, err
	}

	return nil, AssertionError
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

// GetByMd5 gets a file by its md5.
func (s *DFSServer) GetByMd5(ctx context.Context, req *transfer.GetByMd5Req) (*transfer.GetByMd5Rep, error) {
	serviceName := "GetByMd5"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("service: %s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Md5) == 0 || req.Domain <= 0 || req.Size < 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := withDeadline(serviceName, ctx, req, func(c interface{}, r interface{}) (interface{}, error) {
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
	})

	if err != nil {
		return nil, err
	}

	if result, ok := t.(*transfer.GetByMd5Rep); ok {
		return result, nil
	}

	return nil, AssertionError
}

// ExistByMd5 checks existentiality of a file.
func (s *DFSServer) ExistByMd5(ctx context.Context, req *transfer.GetByMd5Req) (*transfer.ExistRep, error) {
	serviceName := "ExistByMd5"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("service: %s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Md5) == 0 || req.Domain <= 0 || req.Size < 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := withDeadline(serviceName, ctx, req, func(c interface{}, r interface{}) (interface{}, error) {
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
	})

	if err != nil {
		return nil, err
	}

	if result, ok := t.(*transfer.ExistRep); ok {
		return result, nil
	}

	return nil, AssertionError
}

// Copy copies a file and returns its fid.
func (s *DFSServer) Copy(ctx context.Context, req *transfer.CopyReq) (*transfer.CopyRep, error) {
	serviceName := "Copy"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("service: %s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.SrcFid) == 0 || req.SrcDomain <= 0 || req.DstDomain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := withDeadline(serviceName, ctx, req, func(c interface{}, r interface{}) (interface{}, error) {
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

		// open source file.
		nh, mh, err := s.selector.getDFSFileHandlerForRead(req.SrcDomain)
		if err != nil {
			return nil, err
		}

		var m fileop.DFSFileHandler
		if mh != nil {
			m = *mh
		}

		_, rf, err := searchFile(req.SrcFid, req.SrcDomain, *nh, m)
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
	})

	if err != nil {
		return nil, err
	}

	result, ok := t.(*transfer.CopyRep)
	if ok {
		return result, nil
	}

	return nil, AssertionError
}

// Stat gets file info with given fid.
func (s *DFSServer) Stat(ctx context.Context, req *transfer.GetFileReq) (*transfer.PutFileRep, error) {
	serviceName := "Stat"
	peerAddr := getPeerAddressString(ctx)
	log.Printf("service: %s, client: %s, %v", serviceName, peerAddr, req)

	if len(req.Id) == 0 || req.Domain <= 0 {
		return nil, fmt.Errorf("invalid request [%v]", req)
	}

	t, err := withDeadline(serviceName, ctx, req, func(c interface{}, r interface{}) (interface{}, error) {
		req, ok := r.(*transfer.GetFileReq)
		if !ok {
			return nil, AssertionError
		}

		nh, mh, err := s.selector.getDFSFileHandlerForRead(req.Domain)
		if err != nil {
			log.Printf("Failed to get handler for read, error: %v", err)
			return nil, err
		}

		var m fileop.DFSFileHandler
		if mh != nil {
			m = *mh
		}

		_, file, err := searchFile(req.Id, req.Domain, *nh, m)
		if err != nil {
			return nil, err
		}
		file.Close()

		return &transfer.PutFileRep{
			File: file.GetFileInfo(),
		}, nil
	})

	if err != nil {
		return nil, err
	}

	result, ok := t.(*transfer.PutFileRep)
	if ok {
		return result, nil
	}

	return nil, AssertionError
}

func (s *DFSServer) registerSelf(lsnAddr string, name string) error {
	log.Printf("Start to register self[%s,%s]", name, lsnAddr)

	rAddr, err := sanitizeLsnAddr(lsnAddr)
	if err != nil {
		return err
	}

	if err := s.register.Register(&discovery.DfsServer{
		Id:     name,
		Uri:    rAddr,
		Status: discovery.DfsServer_ONLINE,
	}); err != nil {
		return err
	}

	log.Printf("Succeeded to register self[%s,%s] on %s ok", name, rAddr, transfer.NodeName)
	return nil
}

// NewDFSServer creates a DFSServer
//
// example:
//  lsnAddr, _ := ResolveTCPAddr("tcp", ":10000")
//  dfsServer, err := NewDFSServer(lsnAddr, "mySite", "shard",
//         "mongodb://192.168.1.15:27017", "192.168.1.16:2181", 3)
func NewDFSServer(lsnAddr net.Addr, name string, dbName string, uri string, zkAddrs string, zkTimeout int) (*DFSServer, error) {
	log.Printf("Try to start DFS server %v on %v\n", name, lsnAddr.String())

	zk := notice.NewDfsZK(strings.Split(zkAddrs, ","), time.Duration(zkTimeout)*time.Millisecond)
	r := disc.NewZKDfsServerRegister(zk)
	server := DFSServer{
		register: r,
		notice:   zk,
	}

	spaceOp, err := metadata.NewSpaceLogOp(dbName, uri)
	if err != nil {
		return nil, err
	}
	server.spaceOp = spaceOp

	eventOp, err := metadata.NewEventOp(dbName, uri)
	if err != nil {
		return nil, err
	}
	server.eventOp = eventOp

	// Create NewMongoMetaOp
	mop, err := metadata.NewMongoMetaOp(dbName, uri)
	if err != nil {
		return nil, err
	}
	server.mOp = mop

	reop, err := recovery.NewRecoveryEventOp(dbName, uri)
	if err != nil {
		return nil, err
	}
	server.reOp = reop

	server.selector, err = NewHandlerSelector(&server)
	log.Printf("Succeeded to initialize storage servers.")

	// Register self.
	regAddr := *RegisterAddr
	if regAddr == "" {
		regAddr = lsnAddr.String()
	}

	if err := server.registerSelf(regAddr, name); err != nil {
		return nil, err
	}

	server.selector.startRevoveryDispatchRoutine()

	server.selector.startShardNoticeRoutine()

	startRateCheckRoutine()

	log.Printf("Succeeded to start DFS server %v.", name)

	return &server, nil
}

func startRateCheckRoutine() {
	go func() {
		// refresh rate every 5 seconds.
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r, err := instrument.GetTransferRateQuantile("GetFile", 0.99)
				if err == nil && r > 0 { // err != nil ignored
					rRate = r
				}

				w, err := instrument.GetTransferRateQuantile("PutFile", 0.99)
				if err == nil && w > 0 { // err != nil ignored
					wRate = w
				}
			}
		}
	}()
}

func checkTimeout(size int64, rate float64, given time.Duration) (time.Duration, error) {
	if rate != 0.0 && size != 0 {
		rate := int64(rate * 1024) // convert unit of rate from kbit/s to bit/s
		size := size * 8           // convert unit of size from bytes to bits
		need := size / rate
		need *= 1e9
		if given.Nanoseconds() < need {
			log.Printf("DEBUG: Deadline exceeded: rate %f, size %d, need %f, given %f", rate, size, need, given.Nanoseconds())
			return time.Duration(need), context.DeadlineExceeded
		}
	}

	return given, nil
}

func getPeerAddressString(ctx context.Context) (peerAddr string) {
	if per, ok := peer.FromContext(ctx); ok {
		peerAddr = per.Addr.String()
	}

	return
}

func getIfcAddr() ([]string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	ifaddrs := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok {
			ip := ipnet.IP
			if !ip.IsLoopback() && ip.To4() != nil {
				ifaddrs = append(ifaddrs, ip.String())
			}
		}
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("get addr error")
	}

	return ifaddrs, nil
}

func sanitizeLsnAddr(lsnAddr string) (string, error) {
	ss := strings.Split(lsnAddr, ":")

	lstPort := "10000"
	if len(ss) > 1 {
		lstPort = ss[len(ss)-1]
	}

	var registerIp string
	ip := net.ParseIP(ss[0])
	if ip != nil && ip.To4() != nil {
		registerIp = ss[0]
	}

	if registerIp == "" {
		lstIps, err := getIfcAddr()
		if err != nil {
			return "", err
		}
		if len(lstIps) == 0 {
			return "", fmt.Errorf("no interface address, use loopback")
		}
		registerIp = lstIps[0]
	}

	return fmt.Sprintf("%s:%s", registerIp, lstPort), nil
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

// withStreamDeadline processes a stream grpc calling with deadline.
func withStreamDeadline(serviceName string, req interface{}, stream interface{}, f func(r interface{}, s interface{}) error) error {
	if streem, ok := stream.(grpc.Stream); ok {
		_, err := withDeadline(serviceName, streem, req, func(stream interface{}, req interface{}) (interface{}, error) {
			err := f(req, stream)
			return nil, err
		})

		return err
	}

	return f(req, stream)
}

// withDeadline processes a normal grpc calling with deadline.
func withDeadline(serviceName string, env interface{}, req interface{}, f func(c interface{}, r interface{}) (interface{}, error)) (r interface{}, e error) {
	startTime := time.Now()

	entry(serviceName)

	defer func() {
		elapse := time.Since(startTime)
		me := &instrument.Measurements{
			Name:  serviceName,
			Value: float64(elapse.Nanoseconds()),
		}

		if se, ok := e.(transport.StreamError); ok && (se.Code == codes.DeadlineExceeded) || (e == context.DeadlineExceeded) {
			instrument.TimeoutHistogram <- me
			glog.Infof("%s, deadline exceeded, %v seconds.", serviceName, elapse.Seconds())
		} else if e != nil {
			instrument.FailedCounter <- me
			glog.Infof("%s error %v, in %v seconds.", serviceName, e, elapse.Seconds())
		} else {
			instrument.SuccessDuration <- me
			glog.Infof("%s finished in %v seconds.", serviceName, elapse.Seconds())
		}

		exit(serviceName)
	}()

	if deadline, ok := getDeadline(env); ok {
		timeout := deadline.Sub(startTime)

		if timeout <= 0 {
			e = context.DeadlineExceeded
			return
		}

		type Result struct {
			r interface{}
			e error
		}
		results := make(chan *Result)

		ticker := time.NewTicker(timeout)
		defer func() {
			ticker.Stop()
		}()

		go func() {
			result := &Result{}
			// Do business.
			result.r, result.e = f(env, req)
			results <- result
			close(results)
		}()

		select {
		case result := <-results:
			r = result.r
			e = result.e
			return
		case <-ticker.C:
			e = context.DeadlineExceeded
			return
		}
	}

	instrument.NoDeadlineCounter <- &instrument.Measurements{
		Name:  serviceName,
		Value: 1,
	}
	return f(env, req)
}

func getDeadline(env interface{}) (deadline time.Time, ok bool) {
	switch t := env.(type) {
	case context.Context:
		deadline, ok = t.Deadline()
	case grpc.Stream:
		deadline, ok = t.Context().Deadline()
	default:
		return
	}
	return
}

func entry(serviceName string) {
	instrument.InProcess <- &instrument.Measurements{
		Name:  serviceName,
		Value: 1,
	}
}

func exit(serviceName string) {
	instrument.InProcess <- &instrument.Measurements{
		Name:  serviceName,
		Value: -1,
	}
}
