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
	"google.golang.org/grpc/peer"
	"gopkg.in/mgo.v2/bson"

	"jingoal/dfs/discovery"
	"jingoal/dfs/fileop"
	"jingoal/dfs/metadata"
	"jingoal/dfs/notice"
	"jingoal/dfs/recovery"
	"jingoal/dfs/transfer"
	"jingoal/dfs/util"
)

var (
	logDir            = flag.String("gluster-log-dir", "/var/log/dfs", "gluster log file dir")
	heartbeatInterval = flag.Int("hb-interval", 5, "time interval in seconds of heart beat")

	bizElapse    = flag.Int("biz-elapse", 0, "biz emulating elapse")
	RegisterAddr = flag.String("register-addr", "", "register address")

	AssertionError = errors.New("Assertion error")
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
	register discovery.Register
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
	t, err := processNormalDeadline("GetChunkSize()", ctx, req, func(ctx interface{}, req interface{}) (interface{}, error) {
		if *bizElapse > 0 {
			// Emulate biz elapse.
			time.Sleep(time.Duration(*bizElapse) * time.Second)
		}

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

func finishRecv(info *transfer.FileInfo, errStr string, stream transfer.FileTransfer_PutFileServer) error {
	if errStr == "" {
		return stream.SendAndClose(
			&transfer.PutFileRep{
				File: info,
			})
	}

	return stream.SendAndClose(
		&transfer.PutFileRep{
			File: &transfer.FileInfo{
				Id: fmt.Sprintf("recv error: %s", errStr),
			},
		})
}

// PutFile puts a file into server.
func (s *DFSServer) PutFile(stream transfer.FileTransfer_PutFileServer) error {
	return processStreamDeadline("PutFile()", nil, stream, func(interface{}, interface{}) error {
		var reqInfo *transfer.FileInfo
		var file fileop.DFSFile
		var length int
		var handler *fileop.DFSFileHandler

		csize := 0
		startTime := time.Now()
		peerAddr := getPeerAddressString(stream.Context())

		for {
			req, err := stream.Recv()
			if err == io.EOF {
				var errStr string
				elapse := time.Now().Sub(startTime).Nanoseconds()
				if reqInfo != nil {
					inf := file.GetFileInfo()
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

					// save a event for create file ok.
					event := &metadata.Event{
						EType:     metadata.SucCreate,
						Timestamp: util.GetTimeInMilliSecond(),
						Domain:    inf.Domain,
						Fid:       inf.Id,
						Elapse:    elapse,
						Description: fmt.Sprintf("%s[PutFile], client: %s, dst: %s, size: %d",
							metadata.SucCreate.String(), peerAddr, (*handler).Name(), length),
					}
					s.eventOp.SaveEvent(event)
					log.Printf("Succeeded to save file: %s, length %d, elapse %d\n", inf, length, elapse)
				} else {
					// TODO(hanyh): save a event for create file error.
					log.Println("Failed to save file: no file info")
					errStr = "no file info"
				}

				return finishRecv(file.GetFileInfo(), errStr, stream)
			}
			if err != nil {
				log.Printf("Failed to save file: %s, error: %v\n", reqInfo, err)
				return err
			}

			if reqInfo == nil {
				reqInfo = req.GetInfo()
				log.Printf("PutFile: file info: %v, client: %s", reqInfo, peerAddr)
				if reqInfo == nil {
					log.Printf("recv error: no file info")
					return errors.New("recv error: no file info")
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
func (s *DFSServer) GetFile(req *transfer.GetFileReq, stream transfer.FileTransfer_GetFileServer) error {
	return processStreamDeadline("GetFile()", req, stream, func(interface{}, interface{}) error {
		peerAddr := getPeerAddressString(stream.Context())

		log.Printf("GetFile: file info: %v, client: %s", req, peerAddr)

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

		var off int64
		b := make([]byte, fileop.DefaultChunkSizeInBytes)
		for {
			length, err := file.Read(b)
			if err == io.EOF {
				log.Printf("Succeeded to read file, %s, length %d", req.Id, off)
				return nil
			}
			if err != nil {
				log.Printf("Failed to read file, %s, error: %v", req.Id, err)
				return err
			}
			if length == 0 {
				log.Printf("Succeeded to read file, %s, length %d", req.Id, off)
				return nil
			}

			err = stream.Send(&transfer.GetFileRep{
				Chunk: &transfer.Chunk{
					Pos:     off,
					Length:  int64(length),
					Payload: b[:length],
				}})
			if err != nil {
				return err
			}

			off += int64(length)
		}
	})
}

// Remove deletes a file.
func (s *DFSServer) RemoveFile(ctx context.Context, req *transfer.RemoveFileReq) (*transfer.RemoveFileRep, error) {
	peerAddr := getPeerAddressString(ctx)

	log.Printf("RemoveFile, file id: %s, domain: %d, client: %s, call stack:\n%v", req.Id, req.Domain,
		peerAddr, req.GetDesc().Desc)

	startTime := time.Now()

	// log the remove command for audit.
	event := &metadata.Event{
		EType:     metadata.CommandDelete,
		Timestamp: util.GetTimeInMilliSecond(),
		Domain:    req.Domain,
		Fid:       req.Id,
		Elapse:    -1,
		Description: fmt.Sprintf("%s, client %s\n%s", metadata.CommandDelete.String(),
			peerAddr, req.GetDesc().Desc),
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
		Elapse:    time.Now().Sub(startTime).Nanoseconds(),
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
	peerAddr := getPeerAddressString(ctx)
	log.Printf("Duplicate, file id: %s, domain: %d, client: %s", req.Id, req.Domain, peerAddr)
	startTime := time.Now()

	did, err := s.duplicate(req.Id, req.Domain)
	if err != nil {
		event := &metadata.Event{
			EType:       metadata.FailDupl,
			Timestamp:   util.GetTimeInMilliSecond(),
			Domain:      req.Domain,
			Fid:         req.Id,
			Description: fmt.Sprintf("%s, client %s", metadata.FailDupl.String(), peerAddr),
			Elapse:      time.Now().Sub(startTime).Nanoseconds(),
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
		Elapse:      time.Now().Sub(startTime).Nanoseconds(),
	}
	s.eventOp.SaveEvent(event)

	return &transfer.DuplicateRep{
		Id: did,
	}, nil
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
	log.Printf("Exist, file id: %s, domain: %d, client: %s", req.Id, req.Domain,
		getPeerAddressString(ctx))

	result, err := s.exist(req.Id, req.Domain)
	if err != nil {
		log.Printf("Failed to exist %s, %d", req.Id, req.Domain)
	}

	return &transfer.ExistRep{
		Result: result,
	}, err
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
	peerAddr := getPeerAddressString(ctx)
	log.Printf("GetByMd5, MD5: %s, domain: %d, size: %d, client: %s",
		req.Md5, req.Domain, req.Size, peerAddr)

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
	log.Printf("ExistByMd5, MD5: %s, domain: %d, size: %d, client: %s", req.Md5, req.Domain, req.Size,
		getPeerAddressString(ctx))

	_, _, err := s.findByMd5(req.Md5, req.Domain, req.Size)
	if err != nil {
		log.Printf("Failed to find file by md5 [%s, %d, %d], error: %v", req.Md5, req.Domain, req.Size, err)
		return nil, err
	}

	return &transfer.ExistRep{
		Result: true,
	}, nil
}

// Copy copies a file and returns its fid.
func (s *DFSServer) Copy(ctx context.Context, req *transfer.CopyReq) (*transfer.CopyRep, error) {
	peerAddr := getPeerAddressString(ctx)

	log.Printf("Copy, srcId: %s, srcDomain: %d, dstDomain: %d, dstUid: %d, dstBiz: %s, client: %s\n",
		req.SrcFid, req.SrcDomain, req.DstDomain, req.DstUid, req.DstBiz, peerAddr)

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
		Elapse:    time.Now().Sub(startTime).Nanoseconds(),
		Description: fmt.Sprintf("%s[Copy], client: %s, srcFid: %s, dst: %s", metadata.SucCreate.String(),
			peerAddr, req.SrcFid, (*handler).Name()),
	}
	s.eventOp.SaveEvent(event)

	return &transfer.CopyRep{
		Fid: inf.Id,
	}, nil
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
	r := discovery.NewZKDfsServerRegister(zk)
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

	log.Printf("Succeeded to start DFS server %v.", name)

	return &server, nil
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

// processStreamDeadline processes a stream grpc calling.
func processStreamDeadline(logDesc string, req interface{}, stream interface{}, f func(r interface{}, s interface{}) error) error {
	var ctx context.Context
	if streem, ok := stream.(grpc.Stream); ok {
		ctx = streem.Context()
		_, err := processNormalDeadline(logDesc, ctx, req, func(stream interface{}, req interface{}) (interface{}, error) {
			err := f(req, stream)
			return nil, err
		})
		return err
	}

	return f(req, stream)
}

// processNormalDeadline processes a normal grpc calling.
func processNormalDeadline(logDesc string, ctx context.Context, req interface{}, f func(c interface{}, r interface{}) (interface{}, error)) (r interface{}, e error) {
	if dl, ok := ctx.Deadline(); ok {
		type Result struct {
			r interface{}
			e error
		}

		// For debug, test glog.
		glog.Infof("%s, deadline %v", logDesc, dl)

		startTime := time.Now()
		timeout := dl.Sub(startTime)

		defer func() {
			if e == context.DeadlineExceeded {
				// TODO(hanyh): monitor this deadline.
				glog.Infof("%s, deadline exceeded, timeout value %v", logDesc, timeout)
			} else {
				glog.Infof("%s, processing in time %v, timeout value %v", logDesc, time.Now().Sub(startTime), timeout)
			}
		}()

		if timeout <= 0 {
			e = context.DeadlineExceeded
			return
		}

		results := make(chan *Result)

		ticker := time.NewTicker(timeout)
		defer func() {
			ticker.Stop()
		}()

		go func() {
			result := &Result{}
			// Do business.
			result.r, result.e = f(ctx, req)

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

	// For debug
	glog.Infof("%s, without deadline", logDesc)
	return f(ctx, req)
}
