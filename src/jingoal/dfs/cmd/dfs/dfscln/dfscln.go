package main

import (
	"crypto/md5"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"gopkg.in/mgo.v2/bson"

	"jingoal/dfs/discovery"
	"jingoal/dfs/transfer"
)

var (
	serverAddr            = flag.String("server-addr", "127.0.0.1:10000", "server address")
	chunkSizeInBytes      = flag.Int("chunk-size", 1024, "chunk size in bytes")
	fileSizeInBytes       = flag.Int("file-size", 1028576, "file size in bytes")
	fileCount             = flag.Int("file-count", 3, "file count")
	domain                = flag.Int64("domain", 2, "domain")
	finalWaitTimeInSecond = flag.Int("final-wait", 10, "the final wait time in seconds")
	compress              = flag.Bool("compress", true, "compressing transfer file")
	version               = flag.Bool("version", false, "print version")

	buildTime = ""

	AssertionError = errors.New("Assertion error")
)

func checkFlags() {
	if buildTime == "" {
		log.Println("Error: Build time not set!")
		os.Exit(0)
	}

	if *version {
		fmt.Printf("Build time: %s\n", buildTime)
		os.Exit(0)
	}
}

// This is a test client for DFSServer, full function client built in Java.
func main() {
	flag.Parse()
	checkFlags()

	gopts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	if *compress {
		gopts = append(gopts, grpc.WithCompressor(grpc.NewGZIPCompressor()))
		gopts = append(gopts, grpc.WithDecompressor(grpc.NewGZIPDecompressor()))
	}

	conn, err := grpc.Dial(*serverAddr, gopts...)
	if err != nil {
		log.Fatal("dial error")
	}

	if err := acceptDfsServer(conn); err != nil {
		log.Fatal("accept DfsServer error %v", err)
	}

	ckSize, err := getChunkSize(conn, 0 /* without timeout */)
	if err != nil {
		log.Fatal("negotiate chunk size error")
	}
	*chunkSizeInBytes = int(ckSize)

	payload := make([]byte, *fileSizeInBytes)

	files := make(chan *transfer.FileInfo, 10000)
	done := make(chan struct{}, *fileCount)

	go func() {
		for i := 0; i < *fileCount; i++ {
			payload[i] = 0x5A
			file, err := writeFile(conn, payload[:], -1 /* timeout depends on callee */)
			if err != nil {
				log.Printf("%v", err)
				continue
			}
			files <- file
		}

		close(files)
	}()

	go func() {
		for file := range files {
			if err := readFile(conn, file, -1 /* timeout depends on callee */); err != nil {
				log.Printf("%v", err)
			}
			done <- struct{}{}
		}
		close(done)
	}()

	for _ = range done {
	}

	if *finalWaitTimeInSecond < 0 {
		select {}
	}

	time.Sleep(time.Duration(*finalWaitTimeInSecond) * time.Second) // For test heartbeat.
}

func acceptDfsServer(conn *grpc.ClientConn) error {
	client := discovery.NewDiscoveryServiceClient(conn)
	stream, err := client.GetDfsServers(context.Background(),
		&discovery.GetDfsServersReq{
			Client: &discovery.DfsClient{
				Id:  "golang-test-client",
				Uri: "127.0.0.1:0000",
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
				}

			// Heartbean can be used for server checking.
			case *discovery.GetDfsServersRep_Hb:
				log.Printf("heartbeat, server timestamp: %d\n", union.Hb.Timestamp)
			}
		}
	}()

	return nil
}

func getChunkSize(conn *grpc.ClientConn, timeout time.Duration) (int64, error) {
	client := transfer.NewFileTransferClient(conn)

	var cancel context.CancelFunc

	if timeout < 0 {
		timeout = 3 * time.Second
	}

	ctx := context.Background()
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}

	rep, err := client.NegotiateChunkSize(ctx,
		&transfer.NegotiateChunkSizeReq{Size: int64(*chunkSizeInBytes)})
	if err != nil {
		if err == context.DeadlineExceeded && cancel != nil {
			cancel()
		}
		return 0, err
	}

	return rep.Size, nil
}

func writeFile(conn *grpc.ClientConn, payload []byte, timeout time.Duration) (*transfer.FileInfo, error) {
	ckSize := int64(*chunkSizeInBytes)

	startTime := time.Now()
	fileInfo := transfer.FileInfo{
		Name:   fmt.Sprintf("%d", time.Now().UnixNano()),
		Size:   int64(len(payload)),
		Domain: *domain,
	}

	if timeout < 0 {
		timeout = time.Duration(calTimeout()) * time.Millisecond
	}

	result, err := processWithTimeout(context.Background(), nil,
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

	var pos int64
	md5 := md5.New()
	for pos < int64(len(payload)) {
		end := pos + ckSize
		if end > int64(len(payload)) {
			end = int64(len(payload))
		}

		p := payload[pos:end]
		md5.Write(p)
		ck := &transfer.Chunk{Pos: pos,
			Length:  end - pos,
			Payload: p,
		}
		req := &transfer.PutFileReq{Info: &fileInfo, Chunk: ck}
		err := stream.Send(req)
		if err != nil {
			return nil, err
		}
		pos = end
	}

	file, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	info := file.GetFile()
	info.Md5 = fmt.Sprintf("%x", md5.Sum(nil))

	if !bson.IsObjectIdHex(info.Id) {
		log.Printf("write file error: %s\n", info.Id)
		return nil, fmt.Errorf(info.Id)
	}

	elapse := int64(time.Now().Sub(startTime).Seconds())
	log.Printf("write file ok, fileid %s, elapse %d\n", info.Id, elapse)

	return info, nil
}

func readFile(conn *grpc.ClientConn, info *transfer.FileInfo, timeout time.Duration) error {
	client := transfer.NewFileTransferClient(conn)

	startTime := time.Now()

	getFileReq := &transfer.GetFileReq{
		Id:     info.Id,
		Domain: info.Domain,
	}

	if timeout < 0 {
		timeout = time.Duration(calTimeout()) * time.Millisecond
	}

	result, err := processWithTimeout(context.Background(), getFileReq,
		func(ctx context.Context, req interface{}, others ...interface{}) (interface{}, error) {
			getFileStream, err := client.GetFile(ctx, getFileReq)
			return getFileStream, err
		},
		timeout,
	)
	if err != nil {
		return err
	}

	getFileStream, ok := result.(transfer.FileTransfer_GetFileClient)
	if !ok {
		return AssertionError
	}

	md5 := md5.New()
	var fileSize int64
	for {
		ck, err := getFileStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		fileSize += ck.GetChunk().Length
		md5.Write(ck.GetChunk().Payload)
	}

	elapse := int64(time.Now().Sub(startTime).Seconds())
	md5Str := fmt.Sprintf("%x", md5.Sum(nil))
	if md5Str == info.Md5 {
		log.Printf("read file ok, fileid %s, elapse %d\n", info.Id, elapse)
	} else {
		log.Printf("read file error, fileid %s, md5 not equals", info.Id)
	}

	return nil
}

func processWithTimeout(ctx context.Context, req interface{},
	f func(context.Context, interface{}, ...interface{}) (interface{}, error),
	timeout time.Duration) (interface{}, error) {

	var tx context.Context
	var cancel context.CancelFunc

	if timeout > 0 {
		log.Printf("DFSClient set timeout to %v", timeout)
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
func calTimeout() int {
	return *fileSizeInBytes * 8 * 1000 / (1073741824 * 8 / 10 / *fileCount)
}
