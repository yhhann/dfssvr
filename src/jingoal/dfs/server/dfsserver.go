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

	"golang.org/x/net/context"

	"jingoal/dfs/discovery"
	"jingoal/dfs/fileop"
	"jingoal/dfs/metadata"
	"jingoal/dfs/recovery"
	"jingoal/dfs/transfer"
)

var (
	logDir            = flag.String("gluster-log-dir", "/var/log/dfs", "gluster log file dir")
	heartbeatInterval = flag.Int("hb-interval", 5, "time interval in seconds of heart beat")
)

const (
	MaxChunkSize = 10240 // Max chunk size in bytes.
	MinChunkSize = 1024  // Min chunk size in bytes.
)

// DFSServer implements DiscoveryServiceServer and FileTransferServer.
type DFSServer struct {
	mOp      metadata.MetaOp
	reOp     *recovery.RecoveryEventOp
	register discovery.Register
	selector *HandlerSelector
}

// GetDfsServers gets a list of DfsServer from server.
func (s *DFSServer) GetDfsServers(req *discovery.GetDfsServersReq, stream discovery.DiscoveryService_GetDfsServersServer) error {
	observer := make(chan struct{}, 100)
	s.register.AddObserver(observer, req.GetClient().String())

	log.Printf("Client connected successfully, %v", req.GetClient().String())

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
	log.Printf("Client connection closed, %v", req.GetClient().String())

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

	return nil
}

// NegotiateChunkSize negotiates chunk size in bytes between client and server.
func (s *DFSServer) NegotiateChunkSize(ctx context.Context, req *transfer.NegotiateChunkSizeReq) (*transfer.NegotiateChunkSizeRep, error) {
	return &transfer.NegotiateChunkSizeRep{
		Size: sanitizeChunkSize(req.Size),
	}, nil
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
	var reqInfo *transfer.FileInfo
	var file fileop.DFSFile

	startTime := time.Now()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			var errStr string
			elapse := time.Now().Sub(startTime).Seconds()
			if reqInfo != nil {
				// TODO(hanyh): save a event for create file ok.
				log.Printf("recv and save file ok, %s, elapse %f\n", reqInfo, elapse)
			} else {
				// TODO(hanyh): save a event for create file error.
				log.Println("recv error: no file info")
				errStr = "no file info"
			}

			return finishRecv(file.GetFileInfo(), errStr, stream)
		}
		if err != nil {
			log.Printf("recv error, file: %s, error: %v\n", reqInfo, err)
			return err
		}

		if reqInfo == nil {
			reqInfo = req.GetInfo()
			if reqInfo == nil {
				log.Printf("recv error: no file info")
				return errors.New("recv error: no file info")
			}

			handler, err := s.selector.getDFSFileHandlerForWrite(reqInfo.Domain)
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

		_, err = file.Write(req.GetChunk().Payload[:])
		if err != nil {
			return err
		}
	}
}

func searchFile(id string, domain int64, nh fileop.DFSFileHandler, mh fileop.DFSFileHandler) (fileop.DFSFile, error) {
	if mh != nil && nh != nil {
		file, err := mh.Open(id, domain)
		if err != nil { // Need not to check mgo.ErrNotFound
			file, err = nh.Open(id, domain)
		}
		return file, err
	} else if mh == nil && nh != nil {
		return nh.Open(id, domain)
	} else {
		return nil, fmt.Errorf("get file error: normal site is nil")
	}
}

// GetFile gets a file from server.
func (s *DFSServer) GetFile(req *transfer.GetFileReq, stream transfer.FileTransfer_GetFileServer) error {
	nh, mh, err := s.selector.getDFSFileHandlerForRead(req.Domain)
	if err != nil {
		log.Printf("get handler for read error: %v", err)
		return err
	}

	var m fileop.DFSFileHandler
	if mh != nil {
		m = *mh
	}

	file, err := searchFile(req.Id, req.Domain, *nh, m)
	if err != nil {
		return err
	}
	defer file.Close()

	var off int64
	b := make([]byte, fileop.DefaultChunkSizeInBytes)
	for {
		length, err := file.Read(b)
		if err == io.EOF {
			log.Printf("read file ok, %s, length %d", req.Id, off)
			return nil
		}
		if err != nil {
			log.Printf("read file error, %s, error: %v", req.Id, err)
			return err
		}
		if length == 0 {
			log.Printf("read file ok, %s, length %d", req.Id, off)
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
}

// Remove deletes a file.
func (s *DFSServer) RemoveFile(ctx context.Context, req *transfer.RemoveFileReq) (*transfer.RemoveFileRep, error) {
	log.Printf("RemoveFile, Request: %v %s %d\n", req.GetDesc().Desc, req.Id, req.Domain)

	// TODO(hanyh): remove file from proper handler.

	rep := transfer.RemoveFileRep{
		Result: true, // For test.
	}

	return &rep, nil
}

// Duplicate duplicates a file, returns a new fid.
func (s *DFSServer) Duplicate(ctx context.Context, req *transfer.DuplicateReq) (*transfer.DuplicateRep, error) {
	log.Printf("Duplicate, Request: %s, %d\n", req.Id, req.Domain)

	// TODO(hanyh): duplicate file from proper handler.

	rep := transfer.DuplicateRep{
		Id: fmt.Sprintf("%s--dupl", req.Id), // For test.
	}
	return &rep, nil
}

// Exist checks existentiality of a file.
func (s *DFSServer) Exist(ctx context.Context, req *transfer.ExistReq) (*transfer.ExistRep, error) {
	log.Printf("Exist, Request: %s, %d\n", req.Id, req.Domain)

	// TODO(hanyh):

	rep := transfer.ExistRep{
		Result: true,
	}
	return &rep, nil
}

// GetByMd5 gets a file by its md5.
func (s *DFSServer) GetByMd5(ctx context.Context, req *transfer.GetByMd5Req) (*transfer.GetByMd5Rep, error) {
	log.Printf("GetByMd5, Request: %s, %d, %d\n", req.Md5, req.Domain, req.Size)

	// TODO(hanyh):

	rep := transfer.GetByMd5Rep{
		Fid: "for-test-id", // For test.
	}
	return &rep, nil
}

// ExistByMd5 checks existentiality of a file.
func (s *DFSServer) ExistByMd5(ctx context.Context, req *transfer.GetByMd5Req) (*transfer.ExistRep, error) {
	log.Printf("ExistByMd5, Request: %s, %d, %d\n", req.Md5, req.Domain, req.Size)

	// TODO(hanyh):

	rep := transfer.ExistRep{
		Result: true, // For test.
	}
	return &rep, nil
}

// Copy copies a file and returns its fid.
func (s *DFSServer) Copy(ctx context.Context, req *transfer.CopyReq) (*transfer.CopyRep, error) {
	log.Printf("Copy, Request: %s, %d, %d, %d, %s\n", req.SrcFid, req.SrcDomain,
		req.DstDomain, req.DstUid, req.DstBiz)

	// TODO(hanyh):

	rep := transfer.CopyRep{
		Fid: fmt.Sprintf("%v--copy", req.SrcFid), // For test.
	}
	return &rep, nil
}

func (s *DFSServer) registerSelf(lsnAddr string, name string) error {
	log.Printf("Start to register self[%s,%s]", name, lsnAddr)

	rAddr, err := s.sanitizeLsnAddr(lsnAddr)
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

func (s *DFSServer) sanitizeLsnAddr(lsnAddr string) (string, error) {
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
		lstIps, err := s.getIfcAddr()
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

func (s *DFSServer) getIfcAddr() ([]string, error) {
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

// NewDFSServer creates a DFSServer
//
// example:
//  lsnAddr, _ := ResolveTCPAddr("tcp", ":10000")
//  dfsServer, err := NewDFSServer(lsnAddr, "mySite", "shard",
//         "mongodb://192.168.1.15:27017", "192.168.1.16:2181", 3)
func NewDFSServer(lsnAddr net.Addr, name string, dbName string, uri string, zkAddr string, timeout int) (*DFSServer, error) {
	log.Printf("Try to start DFS server %v on %v\n", name, lsnAddr.String())

	r := discovery.NewZKDfsServerRegister(zkAddr, time.Duration(timeout)*time.Millisecond)
	server := DFSServer{
		register: r,
	}

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

	// Fill segment data.
	segments := server.mOp.FindAllSegmentsOrderByDomain()
	log.Println("Succeeded to fill segments.")
	for _, seg := range segments {
		log.Printf("segment: [Domain:%d, ns:%s, ms:%s]", seg.Domain, seg.NormalServer, seg.MigrateServer)
	}

	// Initialize storage servers
	shards := server.mOp.FindAllShards()
	server.selector, err = NewHandlerSelector(segments, shards, server.reOp)
	log.Printf("Succeeded to initialize storage servers.")

	// Register self.
	if err := server.registerSelf(lsnAddr.String(), name); err != nil {
		return nil, err
	}

	// routines for healthy check must be started after server registered.
	server.selector.startHealthyCheckRoutine()

	log.Printf("Succeeded to start DFS server %v.", name)

	return &server, nil
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
