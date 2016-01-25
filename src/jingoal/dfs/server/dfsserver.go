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
	"jingoal/dfs/transfer"
)

var (
	logDir = flag.String("gluster-log-dir", "/var/log/dfs", "gluster log file dir")
)

const (
	MaxChunkSize = 10240 // Max chunk size in bytes.
	MinChunkSize = 1024  // Min chunk size in bytes.
)

// DFSServer implements DiscoveryServiceServer and FileTransferServer.
type DFSServer struct {
	mOp      metadata.MetaOp
	register discovery.Register
	selector *HandlerSelector
}

// GetDfsServers gets a list of DfsServer from server.
func (s *DFSServer) GetDfsServers(req *discovery.GetDfsServersReq, stream discovery.DiscoveryService_GetDfsServersServer) error {
	for _, pd := range s.register.GetDfsServerMap() {
		rep := &discovery.GetDfsServersRep{Server: pd}
		if err := stream.Send(rep); err != nil {
			log.Printf("send DfsServer %v to client %s error %v", pd, req.GetClient(), err)
			// TODO(hanyh):process according the err.
		}
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

func (s *DFSServer) registerSelf(lsnAddr string, name string) error {
	log.Printf("start to register self: %s on %s", name, lsnAddr)

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

	log.Printf("register %s - %s ok", name, rAddr)
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
	log.Printf("start dfs server on %v\n", lsnAddr.String())

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

	// Fill segment data.
	segments := server.mOp.FindAllSegmentsOrderByDomain()
	log.Printf("fill segments ok [%+v]", segments)

	// Initialize storage servers
	shards := server.mOp.FindAllShards()
	server.selector, err = NewHandlerSelector(segments, shards)
	log.Printf("initialize the storage servers ok.")

	// Register self.
	if err := server.registerSelf(lsnAddr.String(), name); err != nil {
		return nil, err
	}
	log.Printf("register self ok [%+v], dfs server started.", name)

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
