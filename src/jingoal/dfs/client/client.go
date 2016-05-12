package client

import (
	"crypto/md5"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"jingoal/dfs/discovery"
	"jingoal/dfs/transfer"
)

var (
	serverAddr = flag.String("server-addr", "127.0.0.1:10000", "server address")
	compress   = flag.Bool("compress", false, "compressing transfer file")

	// clientId is the unique id for client, how to assign an id to a client
	// depends upon the client.
	clientId = flag.String("client-id", "id-not-set", "unique client id")

	conn    *grpc.ClientConn
	servers []*discovery.DfsServer

	AssertionError = errors.New("assertion error")
)

// Initialize initializes a client which connected to a single server.
// TODO(hanyh): a handler will be created to represent the endpoint
// connected to a single server. A client holds a handler list,
// and does loadbalance.
func Initialize() {
	gopts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	// TODO(hanyh): Get a list of server addresses with the up 'conn', and then
	// do loadbalance and failover with these servers.

	if *compress {
		gopts = append(gopts, grpc.WithCompressor(grpc.NewGZIPCompressor()))
		gopts = append(gopts, grpc.WithDecompressor(grpc.NewGZIPDecompressor()))
	}

	var err error
	conn, err = grpc.Dial(*serverAddr, gopts...)
	if err != nil {
		log.Fatal("dial error")
	}
}

// AcceptDfsServer accepts change of dfs servers.
// This will be use for loadbalance in the future.
func AcceptDfsServer() error {
	discoveryClient := discovery.NewDiscoveryServiceClient(conn)
	stream, err := discoveryClient.GetDfsServers(context.Background(),
		&discovery.GetDfsServersReq{
			Client: &discovery.DfsClient{
				Id: *clientId,
			},
		})
	if err != nil {
		return err
	}

	go func() {
		for {
			rep, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("accept DfsServer error: %v", err)
				continue
			}

			switch union := rep.GetDfsServerUnion.(type) {
			default:
				log.Printf("acceptDfsServer error, unexpected type %T", union)
			case *discovery.GetDfsServersRep_Sl:
				sl := union.Sl.GetServer()
				for _, server := range sl {
					log.Printf("server %+v\n", server)
					// TODO(hanyh): update server cluster nodes list.
				}
			case *discovery.GetDfsServersRep_Hb:
				// Heartbean can be used for server checking.
				log.Printf("heartbeat, server timestamp: %d\n", union.Hb.Timestamp)
			}
		}
	}()

	return nil
}

// GetChunkSize negotiates a chunk size with server.
func GetChunkSize(chunkSizeInBytes int64, timeout time.Duration) (int64, error) {
	var cancel context.CancelFunc

	ctx := context.Background()
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}

	rep, err := transfer.NewFileTransferClient(conn).NegotiateChunkSize(ctx,
		&transfer.NegotiateChunkSizeReq{
			Size: chunkSizeInBytes,
		})
	if err != nil {
		if err == context.DeadlineExceeded && cancel != nil {
			cancel()
		}
		return 0, err
	}

	return rep.Size, nil
}

// GetByMd5 duplicates a new file with a given md5.
func GetByMd5(md5 string, domain int64, size int64, timeout time.Duration) (string, error) {
	md5Req := transfer.GetByMd5Req{
		Md5:    md5,
		Domain: domain,
		Size:   size,
	}

	t, err := withTimeout("GetByMd5", context.Background(), &md5Req,
		func(ctx context.Context, r interface{}, others ...interface{}) (interface{}, error) {
			req, ok := r.(*transfer.GetByMd5Req)
			if !ok {
				return "", AssertionError
			}

			return transfer.NewFileTransferClient(conn).GetByMd5(ctx, req)
		},
		timeout,
	)

	if err != nil {
		return "", err
	}

	if result, ok := t.(*transfer.GetByMd5Rep); ok {
		return result.Fid, nil
	}

	return "", AssertionError
}

// Duplicate duplicates an entry for an existing file.
func Duplicate(fid string, domain int64, timeout time.Duration) (string, error) {
	dupReq := transfer.DuplicateReq{
		Id:     fid,
		Domain: domain,
	}

	t, err := withTimeout("Duplicate", context.Background(), &dupReq,
		func(ctx context.Context, r interface{}, others ...interface{}) (interface{}, error) {
			req, ok := r.(*transfer.DuplicateReq)
			if !ok {
				return "", AssertionError
			}

			return transfer.NewFileTransferClient(conn).Duplicate(ctx, req)
		},
		timeout,
	)

	if err != nil {
		return "", err
	}

	if result, ok := t.(*transfer.DuplicateRep); ok {
		return result.Id, nil
	}

	return "", AssertionError
}

// Exists returns existence of a specified file by its id.
func Exists(fid string, domain int64, timeout time.Duration) (bool, error) {
	existReq := transfer.ExistReq{
		Id:     fid,
		Domain: domain,
	}

	t, err := withTimeout("Exist", context.Background(), &existReq,
		func(ctx context.Context, r interface{}, others ...interface{}) (interface{}, error) {
			req, ok := r.(*transfer.ExistReq)
			if !ok {
				return false, AssertionError
			}

			return transfer.NewFileTransferClient(conn).Exist(ctx, req)
		},
		timeout,
	)

	if err != nil {
		return false, err
	}

	if result, ok := t.(*transfer.ExistRep); ok {
		return result.Result, nil
	}

	return false, AssertionError
}

// ExistByMd5 returns existence of a specified file by its md5.
func ExistByMd5(md5 string, domain int64, size int64, timeout time.Duration) (bool, error) {
	md5Req := transfer.GetByMd5Req{
		Md5:    md5,
		Domain: domain,
		Size:   size,
	}

	t, err := withTimeout("ExistByMd5", context.Background(), &md5Req,
		func(ctx context.Context, r interface{}, others ...interface{}) (interface{}, error) {
			req, ok := r.(*transfer.GetByMd5Req)
			if !ok {
				return false, AssertionError
			}

			return transfer.NewFileTransferClient(conn).ExistByMd5(ctx, req)
		},
		timeout,
	)

	if err != nil {
		return false, err
	}

	if result, ok := t.(*transfer.ExistRep); ok {
		return result.Result, nil
	}

	return false, AssertionError
}

// Copy copies a file, if dst domain is same as src domain, it will
// call duplicate internally, If not, it will copy file indeedly.
func Copy(fid string, dstDomain int64, srcDomain int64, uid string, biz string, timeout time.Duration) (string, error) {
	userId, err := strconv.Atoi(uid)
	if err != nil {
		return "", err
	}

	copyReq := transfer.CopyReq{
		SrcFid:    fid,
		SrcDomain: srcDomain,
		DstDomain: dstDomain,
		DstUid:    int64(userId),
		DstBiz:    biz,
	}

	t, err := withTimeout("Copy", context.Background(), &copyReq,
		func(ctx context.Context, r interface{}, others ...interface{}) (interface{}, error) {
			req, ok := r.(*transfer.CopyReq)
			if !ok {
				return "", AssertionError
			}

			return transfer.NewFileTransferClient(conn).Copy(ctx, req)
		},
		timeout,
	)

	if err != nil {
		return "", err
	}

	if result, ok := t.(*transfer.CopyRep); ok {
		return result.Fid, nil
	}

	return "", AssertionError
}

// Delete removes a file.
func Delete(fid string, domain int64, timeout time.Duration) error {
	removeFileReq := transfer.RemoveFileReq{
		Id:     fid,
		Domain: domain,
		Desc: &transfer.ClientDescription{
			Desc: "This is a test description",
		},
	}

	t, err := withTimeout("RemoveFile", context.Background(), &removeFileReq,
		func(ctx context.Context, r interface{}, others ...interface{}) (interface{}, error) {
			req, ok := r.(*transfer.RemoveFileReq)
			if !ok {
				return nil, AssertionError
			}

			return transfer.NewFileTransferClient(conn).RemoveFile(ctx, req)
		},
		timeout,
	)
	if err != nil {
		return err
	}

	if result, ok := t.(*transfer.RemoveFileRep); ok {
		log.Printf("Delete file %s, %t", fid, result.Result)
		return nil
	}

	return AssertionError
}

// WriteFile writes a file for given content. Only for test use.
func WriteFile(payload []byte, chunkSize int, domain int64, timeout time.Duration) (*transfer.FileInfo, error) {
	size := int64(len(payload))
	fn := fmt.Sprintf("%d", time.Now().UnixNano())
	writer, err := GetWriter(domain, size, fn, "test-biz", "1001", timeout)
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	var pos int64
	md5 := md5.New()
	for pos < size {
		end := pos + int64(chunkSize)
		if end > size {
			end = size
		}

		p := payload[pos:end]
		md5.Write(p)

		n, err := writer.Write(p)
		if err != nil {
			if err == io.EOF {
				writer.Close()
				return writer.GetFileInfo()
			}
			return nil, err
		}
		pos += int64(n)
	}

	writer.Close()
	return writer.GetFileInfo()
}

// ReadFile reads a file content. Only for test use.
func ReadFile(info *transfer.FileInfo, timeout time.Duration) error {
	startTime := time.Now()

	reader, err := GetReader(info.Domain, info.Id, timeout)
	if err != nil {
		return err
	}

	buf := make([]byte, 8192)
	md5 := md5.New()
	for {
		n, err := reader.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		md5.Write(buf[:n])
	}

	secs := time.Since(startTime).Seconds()
	md5Str := fmt.Sprintf("%x", md5.Sum(nil))
	if md5Str == info.Md5 {
		log.Printf("read file ok, fileid %s, elapse %f second\n", info.Id, secs)
	} else {
		log.Printf("read file error, fileid %s, md5 not equals", info.Id)
	}

	return nil
}

// GetReader returns a io.Reader object.
func GetReader(domain int64, fid string, timeout time.Duration) (*DFSReader, error) {
	req := &transfer.GetFileReq{
		Id:     fid,
		Domain: domain,
	}

	result, err := withTimeout("GetReader", context.Background(), req,
		func(ctx context.Context, r interface{}, others ...interface{}) (interface{}, error) {
			req, ok := r.(*transfer.GetFileReq)
			if !ok {
				return nil, AssertionError
			}

			getFileStream, err := transfer.NewFileTransferClient(conn).GetFile(ctx, req)
			return getFileStream, err
		},
		timeout,
	)
	if err != nil {
		return nil, err
	}

	stream, ok := result.(transfer.FileTransfer_GetFileClient)
	if !ok {
		return nil, AssertionError
	}

	rep, err := stream.Recv()
	if err != nil {
		return nil, err
	}

	info := rep.GetInfo()
	if info == nil {
		return nil, AssertionError
	}
	log.Printf("Succeeded to get reader %s, %d, %d", info.Id, info.Domain, info.Size)

	return NewDFSReader(stream, info), nil
}

// GetWriter returns a io.Writer object.
func GetWriter(domain int64, size int64, filename string, biz string, user string, timeout time.Duration) (*DFSWriter, error) {
	userId, err := strconv.Atoi(user)
	if err != nil {
		return nil, err
	}

	fileInfo := transfer.FileInfo{
		Name:   filename,
		Size:   size,
		Domain: domain,
		User:   int64(userId),
		Biz:    biz,
	}

	result, err := withTimeout("GetWriter", context.Background(), nil,
		func(ctx context.Context, req interface{}, others ...interface{}) (interface{}, error) {
			return transfer.NewFileTransferClient(conn).PutFile(ctx)
		},
		timeout,
	)
	if err != nil {
		return nil, err
	}

	stream, ok := result.(transfer.FileTransfer_PutFileClient)
	if !ok {
		return nil, AssertionError
	}

	writer := NewDFSWriter(&fileInfo, stream)

	return writer, nil
}

func withTimeout(logDesc string, ctx context.Context, req interface{},
	f func(context.Context, interface{}, ...interface{}) (interface{}, error),
	timeout time.Duration) (interface{}, error) {

	var cancel context.CancelFunc
	tx := ctx

	if timeout > 0 {
		log.Printf("%s set timeout to %v", logDesc, timeout)
		tx, cancel = context.WithTimeout(ctx, timeout)
	}

	result, err := f(tx, req)
	if err != nil {
		if err == context.DeadlineExceeded && cancel != nil {
			cancel()
		}
		return nil, err
	}

	return result, nil
}

// calTimeout calculates a time out value in millisecond, only for test.
func calTimeout(fileSizeInBytes int, fileCount int) int {
	return fileSizeInBytes * 8 * 1000 / (1073741824 * 8 / 10 / fileCount)
}
